package index

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	cbor "github.com/fxamacker/cbor/v2"
)

const snapshotFile = "index.cbor"
const chunkIDPrefix = "chunk-"

// --- Private CBOR wire types ---
//
// Integer map keys (keyasint) eliminate per-record field-name overhead.
// cborExtent stores chunk IDs as a uint32 sequence number instead of the full
// "chunk-NNNNNNNN" string, saving ~11 bytes per extent on disk.

type cborInode struct {
	Ino      uint64 `cbor:"0,keyasint"`
	FileType uint32 `cbor:"1,keyasint"`
	Mode     uint32 `cbor:"2,keyasint"`
	Nlink    uint32 `cbor:"3,keyasint"`
	UID      uint32 `cbor:"4,keyasint"`
	GID      uint32 `cbor:"5,keyasint"`
	Size     int64  `cbor:"6,keyasint"`
	Atime    int64  `cbor:"7,keyasint"`
	Mtime    int64  `cbor:"8,keyasint"`
	Ctime    int64  `cbor:"9,keyasint"`
	Blksize  int32  `cbor:"10,keyasint"`
	Blocks   int64  `cbor:"11,keyasint"`
	Rdev     uint32 `cbor:"12,keyasint"`
}

type cborExtent struct {
	ChunkSeq    uint32 `cbor:"0,keyasint"`
	ChunkOffset int64  `cbor:"1,keyasint"`
	Length      int64  `cbor:"2,keyasint"`
	FileOffset  int64  `cbor:"3,keyasint"`
}

type cborDirEntry struct {
	ParentIno uint64 `cbor:"0,keyasint"`
	Name      string `cbor:"1,keyasint"`
	ChildIno  uint64 `cbor:"2,keyasint"`
}

type cborSnapshot struct {
	Inodes         map[uint64]cborInode         `cbor:"inodes"`
	DirEntries     []cborDirEntry               `cbor:"dirEntries"`
	ExtentMap      map[uint64][]cborExtent      `cbor:"extentMap"`
	SymlinkTargets map[uint64]string            `cbor:"symlinkTargets"`
	Xattrs         map[uint64]map[string]string `cbor:"xattrs"`
	NextIno        uint64                       `cbor:"nextIno"`
}

// parseChunkSeq extracts the sequence number from a "chunk-NNNNNNNN" ID.
func parseChunkSeq(id string) (uint32, error) {
	if !strings.HasPrefix(id, chunkIDPrefix) {
		return 0, fmt.Errorf("chunkID %q is not in chunk-NNNNNNNN format", id)
	}
	var seq uint32
	if _, err := fmt.Sscanf(id[len(chunkIDPrefix):], "%d", &seq); err != nil {
		return 0, fmt.Errorf("chunkID %q: %w", id, err)
	}
	return seq, nil
}

func chunkSeqToID(seq uint32) string {
	return fmt.Sprintf("%s%08d", chunkIDPrefix, seq)
}

// toWireSnapshot converts the public Snapshot to the compact CBOR wire form.
func toWireSnapshot(snap *Snapshot) (cborSnapshot, error) {
	ws := cborSnapshot{
		SymlinkTargets: snap.SymlinkTargets,
		Xattrs:         snap.Xattrs,
		NextIno:        snap.NextIno,
		Inodes:         make(map[uint64]cborInode, len(snap.Inodes)),
		DirEntries:     make([]cborDirEntry, len(snap.DirEntries)),
		ExtentMap:      make(map[uint64][]cborExtent, len(snap.ExtentMap)),
	}
	for ino, n := range snap.Inodes {
		ws.Inodes[ino] = cborInode{
			Ino: n.Ino, FileType: uint32(n.FileType), Mode: n.Mode,
			Nlink: n.Nlink, UID: n.UID, GID: n.GID, Size: n.Size,
			Atime: n.Atime, Mtime: n.Mtime, Ctime: n.Ctime,
			Blksize: n.Blksize, Blocks: n.Blocks, Rdev: n.Rdev,
		}
	}
	for i, de := range snap.DirEntries {
		ws.DirEntries[i] = cborDirEntry{
			ParentIno: de.ParentIno, Name: de.Name, ChildIno: de.ChildIno,
		}
	}
	for ino, exts := range snap.ExtentMap {
		wexts := make([]cborExtent, len(exts))
		for i, e := range exts {
			seq, err := parseChunkSeq(e.ChunkID)
			if err != nil {
				return cborSnapshot{}, err
			}
			wexts[i] = cborExtent{
				ChunkSeq: seq, ChunkOffset: e.ChunkOffset,
				Length: e.Length, FileOffset: e.FileOffset,
			}
		}
		ws.ExtentMap[ino] = wexts
	}
	return ws, nil
}

// fromWireSnapshot converts the CBOR wire form back to the public Snapshot.
func fromWireSnapshot(ws cborSnapshot) *Snapshot {
	snap := &Snapshot{
		SymlinkTargets: ws.SymlinkTargets,
		Xattrs:         ws.Xattrs,
		NextIno:        ws.NextIno,
		Inodes:         make(map[uint64]*Inode, len(ws.Inodes)),
		DirEntries:     make([]DirEntry, len(ws.DirEntries)),
		ExtentMap:      make(map[uint64][]Extent, len(ws.ExtentMap)),
	}
	for ino, wi := range ws.Inodes {
		snap.Inodes[ino] = &Inode{
			Ino: wi.Ino, FileType: FileType(wi.FileType), Mode: wi.Mode,
			Nlink: wi.Nlink, UID: wi.UID, GID: wi.GID, Size: wi.Size,
			Atime: wi.Atime, Mtime: wi.Mtime, Ctime: wi.Ctime,
			Blksize: wi.Blksize, Blocks: wi.Blocks, Rdev: wi.Rdev,
		}
	}
	for i, wde := range ws.DirEntries {
		snap.DirEntries[i] = DirEntry{
			ParentIno: wde.ParentIno, Name: wde.Name, ChildIno: wde.ChildIno,
		}
	}
	for ino, wexts := range ws.ExtentMap {
		exts := make([]Extent, len(wexts))
		for i, we := range wexts {
			exts[i] = Extent{
				ChunkID:     chunkSeqToID(we.ChunkSeq),
				ChunkOffset: we.ChunkOffset,
				Length:      we.Length,
				FileOffset:  we.FileOffset,
			}
		}
		snap.ExtentMap[ino] = exts
	}
	return snap
}

// Save serialises the index to a CBOR file in dir, then atomically replaces
// the previous snapshot using a rename so a partial write never corrupts it.
func Save(idx *Index, dir string) error {
	wire, err := toWireSnapshot(idx.Snapshot())
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	data, err := cbor.Marshal(wire)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	tmp := filepath.Join(dir, snapshotFile+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	dst := filepath.Join(dir, snapshotFile)
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}
	idx.MarkClean()
	return nil
}

// Load reads the CBOR snapshot from dir and returns a restored Index.
// Returns os.ErrNotExist (unwrapped) if no snapshot exists yet.
func Load(dir string, blockSize int32) (*Index, error) {
	path := filepath.Join(dir, snapshotFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err // caller checks os.IsNotExist
	}
	var ws cborSnapshot
	if err := cbor.Unmarshal(data, &ws); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return FromSnapshot(fromWireSnapshot(ws), blockSize), nil
}

// Marshal serialises the index to CBOR bytes. Used for object-store persistence
// where a file path is not available.
func Marshal(idx *Index) ([]byte, error) {
	wire, err := toWireSnapshot(idx.Snapshot())
	if err != nil {
		return nil, fmt.Errorf("encode snapshot: %w", err)
	}
	data, err := cbor.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	return data, nil
}

// Unmarshal restores an Index from the CBOR bytes produced by Marshal.
func Unmarshal(data []byte, blockSize int32) (*Index, error) {
	var ws cborSnapshot
	if err := cbor.Unmarshal(data, &ws); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return FromSnapshot(fromWireSnapshot(ws), blockSize), nil
}
