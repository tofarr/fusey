// Package journal implements Fusey's optional edit log.
//
// The journal is an append-only, sharded record of every state-mutating
// operation on the index. It is enabled by FUSEY_JOURNAL_ENABLED and lives
// in the same backing object store as the index, in shards named
// {prefix}journal-NNNNNNNNN.cbor where N is the starting sequence number
// of the batch.
//
// The journal supports two features:
//   - `fusey journal-dump`: a debug subcommand that prints recorded edits.
//   - `fusey mount <mp> --as-of=<ts>`: a read-only mount of the filesystem
//     as it existed at wall-clock time ts. Requires the journal to be
//     present in the backing store; errors if absent.
//
// TouchAtime is deliberately NOT journaled: read access is too high-frequency
// and excluded from remoteStoreDirty in index.qnt for the same reason.
package journal

import (
	"fmt"

	"github.com/tofarr/fusey/internal/index"
)

// CreateInode is recorded when a new inode is allocated. It captures the full
// initial state of the inode so replay can recreate it without needing the
// surrounding context.
type CreateInode struct {
	Ino      uint64         `cbor:"0,keyasint"`
	FileType index.FileType `cbor:"1,keyasint"`
	Mode     uint32         `cbor:"2,keyasint"`
	Nlink    uint32         `cbor:"3,keyasint"`
	UID      uint32         `cbor:"4,keyasint"`
	GID      uint32         `cbor:"5,keyasint"`
	Rdev     uint32         `cbor:"6,keyasint"`
	Atime    int64          `cbor:"7,keyasint"`
	Mtime    int64          `cbor:"8,keyasint"`
	Ctime    int64          `cbor:"9,keyasint"`
	Blksize  int32          `cbor:"10,keyasint"`
	Blocks   int64          `cbor:"11,keyasint"`
}

// SetAttr is recorded for chmod/chown/utimens/truncate. It captures the full
// post-mutation Inode fields so replay can re-apply the same SetAttr, which
// performs the same trim-or-grow logic the live index would have used.
type SetAttr struct {
	Ino   uint64 `cbor:"0,keyasint"`
	Mode  uint32 `cbor:"1,keyasint"`
	UID   uint32 `cbor:"2,keyasint"`
	GID   uint32 `cbor:"3,keyasint"`
	Size  int64  `cbor:"4,keyasint"`
	Atime int64  `cbor:"5,keyasint"`
	Mtime int64  `cbor:"6,keyasint"`
	Ctime int64  `cbor:"7,keyasint"`
	Nlink uint32 `cbor:"8,keyasint"`
	Rdev  uint32 `cbor:"9,keyasint"`
}

// AddDirEntry, RemoveDirEntry, Rename: directory-binding edits.
type AddDirEntry struct {
	ParentIno uint64 `cbor:"0,keyasint"`
	Name      string `cbor:"1,keyasint"`
	ChildIno  uint64 `cbor:"2,keyasint"`
}

type RemoveDirEntry struct {
	ParentIno uint64 `cbor:"0,keyasint"`
	Name      string `cbor:"1,keyasint"`
}

type Rename struct {
	SrcParent uint64 `cbor:"0,keyasint"`
	SrcName   string `cbor:"1,keyasint"`
	DstParent uint64 `cbor:"2,keyasint"`
	DstName   string `cbor:"3,keyasint"`
}

// WriteExtent: a new extent that overwrites (or appends to) a regular file.
// Replay re-applies it via the same WriteExtent path, which handles both
// append and overwrite correctly.
type WriteExtent struct {
	Ino    uint64        `cbor:"0,keyasint"`
	Extent index.Extent  `cbor:"1,keyasint"`
}

// SetSymlink, SetXattr, RemoveXattr, DeleteInode: auxiliary edits.
type SetSymlink struct {
	Ino    uint64 `cbor:"0,keyasint"`
	Target string `cbor:"1,keyasint"`
}

type SetXattr struct {
	Ino   uint64 `cbor:"0,keyasint"`
	Name  string `cbor:"1,keyasint"`
	Value string `cbor:"2,keyasint"`
}

type RemoveXattr struct {
	Ino  uint64 `cbor:"0,keyasint"`
	Name string `cbor:"1,keyasint"`
}

type DeleteInode struct {
	Ino uint64 `cbor:"0,keyasint"`
}

// Op is the tagged-union envelope for a single journaled mutation. CBOR tags
// distinguish each variant; the on-disk encoding is small because each entry
// only carries the fields it needs.
type Op struct {
	CreateInode    *CreateInode    `cbor:"0,keyasint,omitempty"`
	DeleteInode    *DeleteInode    `cbor:"1,keyasint,omitempty"`
	AddDirEntry    *AddDirEntry    `cbor:"2,keyasint,omitempty"`
	RemoveDirEntry *RemoveDirEntry `cbor:"3,keyasint,omitempty"`
	Rename         *Rename         `cbor:"4,keyasint,omitempty"`
	SetAttr        *SetAttr        `cbor:"5,keyasint,omitempty"`
	WriteExtent    *WriteExtent    `cbor:"6,keyasint,omitempty"`
	SetSymlink     *SetSymlink     `cbor:"7,keyasint,omitempty"`
	SetXattr       *SetXattr       `cbor:"8,keyasint,omitempty"`
	RemoveXattr    *RemoveXattr    `cbor:"9,keyasint,omitempty"`
}

// Kind returns a stable string identifying the variant, suitable for the
// human-readable dump format and for testing.
func (o Op) Kind() string {
	switch {
	case o.CreateInode != nil:
		return "CreateInode"
	case o.DeleteInode != nil:
		return "DeleteInode"
	case o.AddDirEntry != nil:
		return "AddDirEntry"
	case o.RemoveDirEntry != nil:
		return "RemoveDirEntry"
	case o.Rename != nil:
		return "Rename"
	case o.SetAttr != nil:
		return "SetAttr"
	case o.WriteExtent != nil:
		return "WriteExtent"
	case o.SetSymlink != nil:
		return "SetSymlink"
	case o.SetXattr != nil:
		return "SetXattr"
	case o.RemoveXattr != nil:
		return "RemoveXattr"
	default:
		return "<empty>"
	}
}

// Exactly one variant must be set. Returns an error if zero or more than one.
func (o Op) validate() error {
	seen := 0
	if o.CreateInode != nil {
		seen++
	}
	if o.DeleteInode != nil {
		seen++
	}
	if o.AddDirEntry != nil {
		seen++
	}
	if o.RemoveDirEntry != nil {
		seen++
	}
	if o.Rename != nil {
		seen++
	}
	if o.SetAttr != nil {
		seen++
	}
	if o.WriteExtent != nil {
		seen++
	}
	if o.SetSymlink != nil {
		seen++
	}
	if o.SetXattr != nil {
		seen++
	}
	if o.RemoveXattr != nil {
		seen++
	}
	switch seen {
	case 0:
		return fmt.Errorf("journal op has no variant set")
	case 1:
		return nil
	default:
		return fmt.Errorf("journal op has %d variants set", seen)
	}
}

// Entry is a timestamped, sequenced journal record. Seq is assigned at append
// time; Ts is the wall-clock nanosecond timestamp captured at the point of
// the original mutation.
type Entry struct {
	Seq uint64 `cbor:"0,keyasint"`
	Ts  int64  `cbor:"1,keyasint"`
	Op  Op     `cbor:"2,keyasint"`
}
