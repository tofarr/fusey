package journal

import (
	"github.com/tofarr/fusey/internal/index"
)

// JournaledIndex wraps an *index.Index and records every state-mutating
// operation to a Journal. Read methods (GetInode, Lookup, Readdir, etc.)
// are promoted from the embedded *index.Index and require no wrapper.
// Write methods are overridden to also append a journal entry.
//
// This is the type the FUSE layer holds; it transparently provides journal
// recording when the journal is enabled and is a no-op pass-through when
// it is disabled.
//
// Concurrency: the underlying *index.Index is already safe for concurrent
// use, and the Journal is too. The wrapper adds no additional locking; the
// race window between the index mutation and the journal append is small
// (a few function calls) and is acceptable — the journal is a debugging
// / replay aid, not a correctness primitive.
//
// For compaction: the compactor needs the underlying *index.Index. Use
// the Inner() accessor to retrieve it.
type JournaledIndex struct {
	*index.Index
	j *Journal
}

// Wrap returns a JournaledIndex that records to j. When j.Enabled() is
// false, the wrapper is a pass-through: the underlying index mutates as
// before, with no journal entries recorded.
func Wrap(idx *index.Index, j *Journal) *JournaledIndex {
	return &JournaledIndex{Index: idx, j: j}
}

// Inner returns the underlying *index.Index. Used by the compactor and by
// the FUSE layer when read-only access to the bare index is needed
// (e.g. for the post-compact remap that does not itself need journaling).
func (ji *JournaledIndex) Inner() *index.Index {
	return ji.Index
}

// --- Mutating methods: each delegates to the index and then appends a
// journal entry. The journal Append is a no-op when the journal is disabled.

// CreateInode allocates a new inode and records a CreateInode op.
func (ji *JournaledIndex) CreateInode(
	fileType index.FileType, mode, uid, gid, rdev uint32, now int64,
) (uint64, error) {
	ino, err := ji.Index.CreateInode(fileType, mode, uid, gid, rdev, now)
	if err != nil {
		return ino, err
	}
	// We need the just-created Inode's fields to fill the journal entry.
	// Read them back; this is racy only in the sense that another writer
	// could have updated the inode between the create and the read, but
	// such updates would themselves be recorded in the journal, so the
	// inconsistency is recoverable.
	if inode, ok := ji.Index.GetInode(ino); ok {
		ji.j.Append(Op{
			CreateInode: &CreateInode{
				Ino: ino, FileType: inode.FileType, Mode: inode.Mode,
				Nlink: inode.Nlink, UID: inode.UID, GID: inode.GID, Rdev: inode.Rdev,
				Atime: inode.Atime, Mtime: inode.Mtime, Ctime: inode.Ctime,
				Blksize: inode.Blksize, Blocks: inode.Blocks,
			},
		}, now)
	}
	return ino, nil
}

// AddDirEntry creates a directory entry and records an AddDirEntry op.
func (ji *JournaledIndex) AddDirEntry(parentIno uint64, name string, childIno uint64, now int64) error {
	if err := ji.Index.AddDirEntry(parentIno, name, childIno, now); err != nil {
		return err
	}
	ji.j.Append(Op{
		AddDirEntry: &AddDirEntry{ParentIno: parentIno, Name: name, ChildIno: childIno},
	}, now)
	return nil
}

// RemoveDirEntry removes a directory entry and records a RemoveDirEntry op.
// If the removal drops nlink to 0, also records a DeleteInode op.
func (ji *JournaledIndex) RemoveDirEntry(parentIno uint64, name string, now int64) error {
	// Look up the child inode number before removal so we can decide
	// whether to emit DeleteInode.
	var childIno uint64
	var wasNlink1 bool
	if inode, ok := ji.Index.GetInode(ji.indexChild(parentIno, name)); ok {
		childIno = inode.Ino
		wasNlink1 = inode.Nlink == 1
	}
	if err := ji.Index.RemoveDirEntry(parentIno, name, now); err != nil {
		return err
	}
	ji.j.Append(Op{
		RemoveDirEntry: &RemoveDirEntry{ParentIno: parentIno, Name: name},
	}, now)
	if wasNlink1 {
		ji.j.Append(Op{DeleteInode: &DeleteInode{Ino: childIno}}, now)
	}
	return nil
}

// Rename renames a directory entry and records a Rename op. If the rename
// replaces an existing destination and that destination's nlink drops to 0,
// also records a DeleteInode op.
func (ji *JournaledIndex) Rename(srcParent uint64, srcName string, dstParent uint64, dstName string, now int64) error {
	// Look up the destination (if any) before the rename to detect
	// replacement and potential deletion.
	var replacedIno uint64
	var replacedNlink1 bool
	if ino, ok := ji.Index.Lookup(dstParent, dstName); ok {
		replacedIno = ino
		if inode, ok := ji.Index.GetInode(ino); ok && inode.Nlink == 1 {
			replacedNlink1 = true
		}
	}
	if err := ji.Index.Rename(srcParent, srcName, dstParent, dstName, now); err != nil {
		return err
	}
	ji.j.Append(Op{
		Rename: &Rename{
			SrcParent: srcParent, SrcName: srcName,
			DstParent: dstParent, DstName: dstName,
		},
	}, now)
	if replacedNlink1 {
		ji.j.Append(Op{DeleteInode: &DeleteInode{Ino: replacedIno}}, now)
	}
	return nil
}

// SetAttr updates inode metadata and records a SetAttr op. Size, mode, etc.
// are captured directly from the args.
func (ji *JournaledIndex) SetAttr(
	ino uint64, mode, uid, gid *uint32, size *int64,
	atime, mtime *int64, now int64,
) error {
	// Capture the post-mutation values for the journal entry. We compute
	// them from the args (with fallbacks to the current state for nil
	// pointers) so the journal entry matches what the live index will
	// have after this call.
	inode, ok := ji.Index.GetInode(ino)
	if !ok {
		// Let the underlying index return the appropriate error.
		return ji.Index.SetAttr(ino, mode, uid, gid, size, atime, mtime, now)
	}
	if err := ji.Index.SetAttr(ino, mode, uid, gid, size, atime, mtime, now); err != nil {
		return err
	}
	resolved := inode
	if mode != nil {
		resolved.Mode = *mode
	}
	if uid != nil {
		resolved.UID = *uid
	}
	if gid != nil {
		resolved.GID = *gid
	}
	if size != nil {
		resolved.Size = *size
	}
	if atime != nil {
		resolved.Atime = *atime
	}
	if mtime != nil {
		resolved.Mtime = *mtime
	}
	resolved.Ctime = now
	ji.j.Append(Op{
		SetAttr: &SetAttr{
			Ino: ino, Mode: resolved.Mode, UID: resolved.UID, GID: resolved.GID,
			Size: resolved.Size, Atime: resolved.Atime, Mtime: resolved.Mtime, Ctime: now,
			Nlink: resolved.Nlink, Rdev: resolved.Rdev,
		},
	}, now)
	return nil
}

// AppendExtent records an extent appended to a regular file. The
// corresponding journal op is WriteExtent (the live index's AppendExtent
// is a strict subset of WriteExtent; the replay code uses WriteExtent).
func (ji *JournaledIndex) AppendExtent(ino uint64, ext index.Extent, now int64) error {
	if err := ji.Index.AppendExtent(ino, ext, now); err != nil {
		return err
	}
	ji.j.Append(Op{WriteExtent: &WriteExtent{Ino: ino, Extent: ext}}, now)
	return nil
}

// WriteExtent records an extent that overwrites part of a regular file.
func (ji *JournaledIndex) WriteExtent(ino uint64, ext index.Extent, now int64) error {
	if err := ji.Index.WriteExtent(ino, ext, now); err != nil {
		return err
	}
	ji.j.Append(Op{WriteExtent: &WriteExtent{Ino: ino, Extent: ext}}, now)
	return nil
}

// SetSymlink records the target of a symlink and a SetSymlink op.
func (ji *JournaledIndex) SetSymlink(ino uint64, target string, now int64) error {
	if err := ji.Index.SetSymlink(ino, target, now); err != nil {
		return err
	}
	ji.j.Append(Op{SetSymlink: &SetSymlink{Ino: ino, Target: target}}, now)
	return nil
}

// SetXattr records a SetXattr op.
func (ji *JournaledIndex) SetXattr(ino uint64, name, value string, now int64) error {
	if err := ji.Index.SetXattr(ino, name, value, now); err != nil {
		return err
	}
	ji.j.Append(Op{SetXattr: &SetXattr{Ino: ino, Name: name, Value: value}}, now)
	return nil
}

// RemoveXattr records a RemoveXattr op.
func (ji *JournaledIndex) RemoveXattr(ino uint64, name string, now int64) error {
	if err := ji.Index.RemoveXattr(ino, name, now); err != nil {
		return err
	}
	ji.j.Append(Op{RemoveXattr: &RemoveXattr{Ino: ino, Name: name}}, now)
	return nil
}

// TouchAtime deliberately does NOT journal anything. Matches the spec:
// read access is too high-frequency and has no replay value.

// indexChild returns the child inode number for (parentIno, name), or 0
// if not found. Used to determine if RemoveDirEntry will free the inode.
func (ji *JournaledIndex) indexChild(parentIno uint64, name string) uint64 {
	ino, _ := ji.Index.Lookup(parentIno, name)
	return ino
}
