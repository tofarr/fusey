// Package index implements the in-memory filesystem index described by
// index.qnt and filesystem.qnt. It tracks inodes, directory entries, extent
// maps, symlink targets, and extended attributes, and enforces the invariants
// proved in the Quint specification.
package index

import (
	"fmt"
	"sync"
	"time"
)

const RootIno uint64 = 1

// ErrExist is returned by the atomic make-node operations when the target name
// already exists in the parent directory.
var ErrExist = fmt.Errorf("entry already exists")

// Index is the in-memory filesystem index. All public methods are safe for
// concurrent use. The index tracks whether it is dirty relative to its last
// on-disk snapshot (see persist.go).
type Index struct {
	mu sync.RWMutex

	inodes    map[uint64]*Inode
	dirIndex  map[uint64]map[string]uint64 // parentIno → name → childIno
	extents   map[uint64][]Extent
	symlinks  map[uint64]string
	xattrs    map[uint64]map[string]string
	nextIno   uint64
	blockSize int32

	dirty bool
}

// New creates an empty Index with a root directory inode.
func New(blockSize int32) *Index {
	now := time.Now().UnixNano()
	root := &Inode{
		Ino:      RootIno,
		FileType: Directory,
		Mode:     0o755,
		Nlink:    1,
		Blksize:  blockSize,
		Atime:    now,
		Mtime:    now,
		Ctime:    now,
	}
	idx := &Index{
		inodes:    map[uint64]*Inode{RootIno: root},
		dirIndex:  map[uint64]map[string]uint64{RootIno: {}},
		extents:   map[uint64][]Extent{},
		symlinks:  map[uint64]string{},
		xattrs:    map[uint64]map[string]string{},
		nextIno:   RootIno + 1,
		blockSize: blockSize,
	}
	return idx
}

// FromSnapshot restores an index from a persisted snapshot.
func FromSnapshot(snap *Snapshot, blockSize int32) *Index {
	idx := &Index{
		inodes:    snap.Inodes,
		dirIndex:  make(map[uint64]map[string]uint64),
		extents:   snap.ExtentMap,
		symlinks:  snap.SymlinkTargets,
		xattrs:    snap.Xattrs,
		nextIno:   snap.NextIno,
		blockSize: blockSize,
	}
	if idx.inodes == nil {
		idx.inodes = map[uint64]*Inode{}
	}
	if idx.extents == nil {
		idx.extents = map[uint64][]Extent{}
	}
	if idx.symlinks == nil {
		idx.symlinks = map[uint64]string{}
	}
	if idx.xattrs == nil {
		idx.xattrs = map[uint64]map[string]string{}
	}
	// Rebuild dirIndex from the flat DirEntry slice.
	for _, e := range snap.DirEntries {
		if idx.dirIndex[e.ParentIno] == nil {
			idx.dirIndex[e.ParentIno] = map[string]uint64{}
		}
		idx.dirIndex[e.ParentIno][e.Name] = e.ChildIno
	}
	// Ensure every directory inode has an entry in dirIndex.
	for ino, inode := range idx.inodes {
		if inode.FileType == Directory {
			if idx.dirIndex[ino] == nil {
				idx.dirIndex[ino] = map[string]uint64{}
			}
		}
	}
	return idx
}

// Snapshot returns a consistent point-in-time copy suitable for persistence.
func (idx *Index) Snapshot() *Snapshot {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	snap := &Snapshot{
		Inodes:         make(map[uint64]*Inode, len(idx.inodes)),
		DirEntries:     []DirEntry{},
		ExtentMap:      make(map[uint64][]Extent, len(idx.extents)),
		SymlinkTargets: make(map[uint64]string, len(idx.symlinks)),
		Xattrs:         make(map[uint64]map[string]string, len(idx.xattrs)),
		NextIno:        idx.nextIno,
	}
	for ino, inode := range idx.inodes {
		cp := *inode
		snap.Inodes[ino] = &cp
	}
	for parentIno, children := range idx.dirIndex {
		for name, childIno := range children {
			snap.DirEntries = append(snap.DirEntries, DirEntry{
				ParentIno: parentIno,
				Name:      name,
				ChildIno:  childIno,
			})
		}
	}
	for ino, exts := range idx.extents {
		cp := make([]Extent, len(exts))
		copy(cp, exts)
		snap.ExtentMap[ino] = cp
	}
	for ino, target := range idx.symlinks {
		snap.SymlinkTargets[ino] = target
	}
	for ino, attrs := range idx.xattrs {
		m := make(map[string]string, len(attrs))
		for k, v := range attrs {
			m[k] = v
		}
		snap.Xattrs[ino] = m
	}
	return snap
}

// IsDirty reports whether the index has changed since the last snapshot.
func (idx *Index) IsDirty() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.dirty
}

// MarkClean resets the dirty flag (called after a successful persist).
func (idx *Index) MarkClean() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.dirty = false
}

// --- Read operations (RLock) ---

// GetInode returns a copy of the inode for ino, or false if it does not exist.
func (idx *Index) GetInode(ino uint64) (Inode, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	inode, ok := idx.inodes[ino]
	if !ok {
		return Inode{}, false
	}
	return *inode, true
}

// Lookup returns the child inode number for (parentIno, name), or 0, false.
func (idx *Index) Lookup(parentIno uint64, name string) (uint64, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	children, ok := idx.dirIndex[parentIno]
	if !ok {
		return 0, false
	}
	childIno, ok := children[name]
	return childIno, ok
}

// Readdir returns all (name, childIno) pairs in the given directory.
func (idx *Index) Readdir(ino uint64) ([]DirEntry, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	inode, ok := idx.inodes[ino]
	if !ok {
		return nil, fmt.Errorf("inode %d not found", ino)
	}
	if inode.FileType != Directory {
		return nil, fmt.Errorf("inode %d is not a directory", ino)
	}
	children := idx.dirIndex[ino]
	entries := make([]DirEntry, 0, len(children))
	for name, childIno := range children {
		entries = append(entries, DirEntry{ParentIno: ino, Name: name, ChildIno: childIno})
	}
	return entries, nil
}

// GetExtents returns the ordered extent list for a regular file.
func (idx *Index) GetExtents(ino uint64) ([]Extent, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	exts, ok := idx.extents[ino]
	if !ok {
		return nil, false
	}
	cp := make([]Extent, len(exts))
	copy(cp, exts)
	return cp, true
}

// GetSymlink returns the target of a symlink inode.
func (idx *Index) GetSymlink(ino uint64) (string, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	target, ok := idx.symlinks[ino]
	return target, ok
}

// GetXattrs returns a copy of the extended attributes for an inode.
func (idx *Index) GetXattrs(ino uint64) map[string]string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	attrs := idx.xattrs[ino]
	cp := make(map[string]string, len(attrs))
	for k, v := range attrs {
		cp[k] = v
	}
	return cp
}

// --- Write operations (Lock) ---

// CreateInode allocates a new inode and returns its number.
// The caller must call AddDirEntry to make it reachable.
func (idx *Index) CreateInode(fileType FileType, mode, uid, gid, rdev uint32, now int64) (uint64, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	ino := idx.nextIno
	idx.nextIno++
	// Directories start with nlink=1 for their own "." entry.
	// Regular files and symlinks start at 0; AddDirEntry brings them to 1.
	startNlink := uint32(0)
	if fileType == Directory {
		startNlink = 1
	}
	inode := &Inode{
		Ino:      ino,
		FileType: fileType,
		Mode:     mode,
		Nlink:    startNlink,
		UID:      uid,
		GID:      gid,
		Blksize:  idx.blockSize,
		Atime:    now,
		Mtime:    now,
		Ctime:    now,
		Rdev:     rdev,
	}
	idx.inodes[ino] = inode
	if fileType == Regular {
		idx.extents[ino] = []Extent{}
	}
	if fileType == Directory {
		idx.dirIndex[ino] = map[string]uint64{}
	}
	idx.dirty = true
	return ino, nil
}

// AddDirEntry creates a directory entry (parentIno, name) → childIno and
// increments the child's nlink. It also updates the parent's mtime/ctime.
func (idx *Index) AddDirEntry(parentIno uint64, name string, childIno uint64, now int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	parent, ok := idx.inodes[parentIno]
	if !ok {
		return fmt.Errorf("parent inode %d not found", parentIno)
	}
	if parent.FileType != Directory {
		return fmt.Errorf("inode %d is not a directory", parentIno)
	}
	child, ok := idx.inodes[childIno]
	if !ok {
		return fmt.Errorf("child inode %d not found", childIno)
	}
	if _, exists := idx.dirIndex[parentIno][name]; exists {
		return fmt.Errorf("entry %q already exists in directory %d", name, parentIno)
	}
	idx.dirIndex[parentIno][name] = childIno
	child.Nlink++
	child.Ctime = now
	parent.Mtime = now
	parent.Ctime = now
	idx.dirty = true
	return nil
}

// MakeFile atomically allocates a new regular-file inode, links it into
// parentIno under name, and returns its inode number. It returns ErrExist if
// name is already present in the parent directory.
//
// Using this instead of separate CreateInode + AddDirEntry calls eliminates the
// window in which a goroutine could observe a newly allocated inode with nlink=0
// that has no directory entry — an orphan that would persist in the index
// indefinitely if AddDirEntry subsequently failed (e.g. due to a concurrent
// create of the same name).
func (idx *Index) MakeFile(parentIno uint64, name string, mode, uid, gid, rdev uint32, now int64) (uint64, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	parent, ok := idx.inodes[parentIno]
	if !ok {
		return 0, fmt.Errorf("parent inode %d not found", parentIno)
	}
	if parent.FileType != Directory {
		return 0, fmt.Errorf("inode %d is not a directory", parentIno)
	}
	if _, exists := idx.dirIndex[parentIno][name]; exists {
		return 0, ErrExist
	}

	ino := idx.nextIno
	idx.nextIno++
	idx.inodes[ino] = &Inode{
		Ino:      ino,
		FileType: Regular,
		Mode:     mode,
		Nlink:    1,
		UID:      uid,
		GID:      gid,
		Blksize:  idx.blockSize,
		Atime:    now,
		Mtime:    now,
		Ctime:    now,
		Rdev:     rdev,
	}
	idx.extents[ino] = []Extent{}
	idx.dirIndex[parentIno][name] = ino
	parent.Mtime = now
	parent.Ctime = now
	idx.dirty = true
	return ino, nil
}

// MakeDir atomically allocates a new directory inode and links it into
// parentIno under name. It returns ErrExist if name already exists.
func (idx *Index) MakeDir(parentIno uint64, name string, mode, uid, gid uint32, now int64) (uint64, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	parent, ok := idx.inodes[parentIno]
	if !ok {
		return 0, fmt.Errorf("parent inode %d not found", parentIno)
	}
	if parent.FileType != Directory {
		return 0, fmt.Errorf("inode %d is not a directory", parentIno)
	}
	if _, exists := idx.dirIndex[parentIno][name]; exists {
		return 0, ErrExist
	}

	ino := idx.nextIno
	idx.nextIno++
	// Nlink=2: one for the parent directory entry, one for the "." self-link.
	idx.inodes[ino] = &Inode{
		Ino:      ino,
		FileType: Directory,
		Mode:     mode,
		Nlink:    2,
		UID:      uid,
		GID:      gid,
		Blksize:  idx.blockSize,
		Atime:    now,
		Mtime:    now,
		Ctime:    now,
	}
	idx.dirIndex[ino] = map[string]uint64{}
	idx.dirIndex[parentIno][name] = ino
	parent.Mtime = now
	parent.Ctime = now
	idx.dirty = true
	return ino, nil
}

// MakeSymlink atomically allocates a new symlink inode pointing at target and
// links it into parentIno under name. It returns ErrExist if name already
// exists.
func (idx *Index) MakeSymlink(parentIno uint64, name, target string, uid, gid uint32, now int64) (uint64, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	parent, ok := idx.inodes[parentIno]
	if !ok {
		return 0, fmt.Errorf("parent inode %d not found", parentIno)
	}
	if parent.FileType != Directory {
		return 0, fmt.Errorf("inode %d is not a directory", parentIno)
	}
	if _, exists := idx.dirIndex[parentIno][name]; exists {
		return 0, ErrExist
	}

	ino := idx.nextIno
	idx.nextIno++
	idx.inodes[ino] = &Inode{
		Ino:      ino,
		FileType: Symlink,
		Mode:     0o777,
		Nlink:    1,
		UID:      uid,
		GID:      gid,
		Blksize:  idx.blockSize,
		Atime:    now,
		Mtime:    now,
		Ctime:    now,
		Size:     int64(len(target)),
	}
	idx.symlinks[ino] = target
	idx.dirIndex[parentIno][name] = ino
	parent.Mtime = now
	parent.Ctime = now
	idx.dirty = true
	return ino, nil
}

// RemoveDirEntry removes (parentIno, name), decrements the child's nlink, and
// frees the inode (and its extents) if nlink reaches zero.
func (idx *Index) RemoveDirEntry(parentIno uint64, name string, now int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	parent, ok := idx.inodes[parentIno]
	if !ok {
		return fmt.Errorf("parent inode %d not found", parentIno)
	}
	childIno, ok := idx.dirIndex[parentIno][name]
	if !ok {
		return fmt.Errorf("entry %q not found in directory %d", name, parentIno)
	}
	child := idx.inodes[childIno]

	// Directories must be empty.
	if child.FileType == Directory {
		if len(idx.dirIndex[childIno]) > 0 {
			return fmt.Errorf("directory %d is not empty", childIno)
		}
	}

	delete(idx.dirIndex[parentIno], name)
	child.Nlink--
	// For directories, the "." self-link accounts for nlink=1 with no
	// external references. Free the inode when no external links remain:
	// that is nlink==0 for files, nlink==1 for directories.
	freeThreshold := uint32(0)
	if child.FileType == Directory {
		freeThreshold = 1
	}
	if child.Nlink <= freeThreshold {
		delete(idx.inodes, childIno)
		delete(idx.extents, childIno)
		delete(idx.symlinks, childIno)
		delete(idx.xattrs, childIno)
		delete(idx.dirIndex, childIno)
	} else {
		child.Ctime = now
	}
	parent.Mtime = now
	parent.Ctime = now
	idx.dirty = true
	return nil
}

// Rename implements POSIX rename(2): atomically moves srcName within srcParent
// to dstName within dstParent, replacing any existing destination entry.
func (idx *Index) Rename(srcParent uint64, srcName string, dstParent uint64, dstName string, now int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	srcChildIno, ok := idx.dirIndex[srcParent][srcName]
	if !ok {
		return fmt.Errorf("source %q not found in directory %d", srcName, srcParent)
	}
	srcChild := idx.inodes[srcChildIno]

	// If destination exists, remove it first (must be empty if a directory).
	if dstChildIno, exists := idx.dirIndex[dstParent][dstName]; exists {
		dstChild := idx.inodes[dstChildIno]
		if dstChild.FileType == Directory && len(idx.dirIndex[dstChildIno]) > 0 {
			return fmt.Errorf("destination directory %d is not empty", dstChildIno)
		}
		dstChild.Nlink--
		dstFreeThreshold := uint32(0)
		if dstChild.FileType == Directory {
			dstFreeThreshold = 1
		}
		if dstChild.Nlink <= dstFreeThreshold {
			delete(idx.inodes, dstChildIno)
			delete(idx.extents, dstChildIno)
			delete(idx.symlinks, dstChildIno)
			delete(idx.xattrs, dstChildIno)
			delete(idx.dirIndex, dstChildIno)
		}
	}

	delete(idx.dirIndex[srcParent], srcName)
	idx.dirIndex[dstParent][dstName] = srcChildIno
	srcChild.Ctime = now
	if parent := idx.inodes[srcParent]; parent != nil {
		parent.Mtime = now
		parent.Ctime = now
	}
	if dstParent != srcParent {
		if parent := idx.inodes[dstParent]; parent != nil {
			parent.Mtime = now
			parent.Ctime = now
		}
	}
	idx.dirty = true
	return nil
}

// SetAttr updates inode metadata. size < 0 means do not change size.
// If the new size is smaller than the current size, extents are trimmed.
func (idx *Index) SetAttr(ino uint64, mode *uint32, uid, gid *uint32, size *int64, atime, mtime *int64, now int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	inode, ok := idx.inodes[ino]
	if !ok {
		return fmt.Errorf("inode %d not found", ino)
	}
	if mode != nil {
		inode.Mode = *mode
	}
	if uid != nil {
		inode.UID = *uid
	}
	if gid != nil {
		inode.GID = *gid
	}
	if atime != nil {
		inode.Atime = *atime
	}
	if mtime != nil {
		inode.Mtime = *mtime
	}
	if size != nil && *size != inode.Size {
		newSize := *size
		if inode.FileType == Regular {
			idx.trimExtents(ino, newSize)
		}
		inode.Size = newSize
		inode.Blocks = blocks512(newSize)
		inode.Mtime = now
	}
	inode.Ctime = now
	idx.dirty = true
	return nil
}

// AppendExtent records a new extent for ino and updates its size and block count.
// The extent must begin at inode.Size (sequential writes).
// For random-write / overwrite semantics, call WriteExtent instead.
func (idx *Index) AppendExtent(ino uint64, ext Extent, now int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	inode, ok := idx.inodes[ino]
	if !ok {
		return fmt.Errorf("inode %d not found", ino)
	}
	if inode.FileType != Regular {
		return fmt.Errorf("inode %d is not a regular file", ino)
	}
	idx.extents[ino] = append(idx.extents[ino], ext)
	newSize := ext.FileOffset + ext.Length
	if newSize > inode.Size {
		inode.Size = newSize
		inode.Blocks = blocks512(newSize)
	}
	inode.Mtime = now
	inode.Ctime = now
	idx.dirty = true
	return nil
}

// WriteExtent records a new extent for ino at an arbitrary file offset,
// orphaning any existing extents that overlap the written range.
// This implements the copy-on-write overwrite semantics from filesystem.qnt.
func (idx *Index) WriteExtent(ino uint64, ext Extent, now int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	inode, ok := idx.inodes[ino]
	if !ok {
		return fmt.Errorf("inode %d not found", ino)
	}
	if inode.FileType != Regular {
		return fmt.Errorf("inode %d is not a regular file", ino)
	}

	end := ext.FileOffset + ext.Length
	surviving := idx.extents[ino][:0]
	for _, e := range idx.extents[ino] {
		eEnd := e.FileOffset + e.Length
		if eEnd <= ext.FileOffset || e.FileOffset >= end {
			// No overlap: keep as-is.
			surviving = append(surviving, e)
			continue
		}
		// Trim head portion that precedes the write.
		if e.FileOffset < ext.FileOffset {
			surviving = append(surviving, Extent{
				ChunkID:     e.ChunkID,
				ChunkOffset: e.ChunkOffset,
				Length:      ext.FileOffset - e.FileOffset,
				FileOffset:  e.FileOffset,
			})
		}
		// Trim tail portion that follows the write.
		if eEnd > end {
			surviving = append(surviving, Extent{
				ChunkID:     e.ChunkID,
				ChunkOffset: e.ChunkOffset + (end - e.FileOffset),
				Length:      eEnd - end,
				FileOffset:  end,
			})
		}
	}
	surviving = append(surviving, ext)
	idx.extents[ino] = surviving

	if end > inode.Size {
		inode.Size = end
		inode.Blocks = blocks512(end)
	}
	inode.Mtime = now
	inode.Ctime = now
	idx.dirty = true
	return nil
}

// SetSymlink records the target for a symlink inode and sets its size.
func (idx *Index) SetSymlink(ino uint64, target string, now int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	inode, ok := idx.inodes[ino]
	if !ok {
		return fmt.Errorf("inode %d not found", ino)
	}
	if inode.FileType != Symlink {
		return fmt.Errorf("inode %d is not a symlink", ino)
	}
	idx.symlinks[ino] = target
	inode.Size = int64(len(target))
	inode.Ctime = now
	idx.dirty = true
	return nil
}

// SetXattr sets or replaces a single extended attribute on an inode.
func (idx *Index) SetXattr(ino uint64, name, value string, now int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	inode, ok := idx.inodes[ino]
	if !ok {
		return fmt.Errorf("inode %d not found", ino)
	}
	if idx.xattrs[ino] == nil {
		idx.xattrs[ino] = map[string]string{}
	}
	idx.xattrs[ino][name] = value
	inode.Ctime = now
	idx.dirty = true
	return nil
}

// RemoveXattr removes a single extended attribute from an inode.
func (idx *Index) RemoveXattr(ino uint64, name string, now int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	inode, ok := idx.inodes[ino]
	if !ok {
		return fmt.Errorf("inode %d not found", ino)
	}
	attrs := idx.xattrs[ino]
	if _, ok := attrs[name]; !ok {
		return fmt.Errorf("xattr %q not found on inode %d", name, ino)
	}
	delete(attrs, name)
	inode.Ctime = now
	idx.dirty = true
	return nil
}

// TouchAtime updates the access time of an inode.
func (idx *Index) TouchAtime(ino uint64, now int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if inode, ok := idx.inodes[ino]; ok {
		inode.Atime = now
		idx.dirty = true
	}
}

// GetParentIno returns the inode number of the directory containing ino,
// scanning the directory index. Returns ino itself for the root.
func (idx *Index) GetParentIno(ino uint64) uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if ino == RootIno {
		return RootIno
	}
	for parentIno, children := range idx.dirIndex {
		for _, childIno := range children {
			if childIno == ino {
				return parentIno
			}
		}
	}
	return ino
}

// BlockSz returns the configured block size for this index instance.
func (idx *Index) BlockSz() int32 {
	return idx.blockSize
}

// UsedBytes returns the sum of the Size field across all regular-file inodes.
// This is the value reported to the kernel as used space in statfs.
func (idx *Index) UsedBytes() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var total int64
	for _, inode := range idx.inodes {
		if inode.FileType == Regular {
			total += inode.Size
		}
	}
	return total
}

// LiveExtents returns the set of all extents currently referenced by any inode.
// Used by the compactor to identify orphaned chunk data.
func (idx *Index) LiveExtents() []Extent {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var all []Extent
	for _, exts := range idx.extents {
		all = append(all, exts...)
	}
	return all
}

// RemapExtents replaces old extent references with new ones in the extent map.
// remap maps (chunkID, chunkOffset) → new Extent. Called by the compactor
// after writing compacted chunks, before deleting the originals.
func (idx *Index) RemapExtents(remap map[[2]interface{}]Extent) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for ino, exts := range idx.extents {
		for i, e := range exts {
			key := [2]interface{}{e.ChunkID, e.ChunkOffset}
			if newExt, ok := remap[key]; ok {
				exts[i] = newExt
			}
		}
		idx.extents[ino] = exts
	}
	idx.dirty = true
}

// trimExtents removes or trims extents beyond newSize (called with lock held).
func (idx *Index) trimExtents(ino uint64, newSize int64) {
	var kept []Extent
	for _, e := range idx.extents[ino] {
		if e.FileOffset >= newSize {
			continue // fully beyond new size: orphaned
		}
		if e.FileOffset+e.Length > newSize {
			// Partially beyond: trim length.
			e.Length = newSize - e.FileOffset
		}
		kept = append(kept, e)
	}
	idx.extents[ino] = kept
}
