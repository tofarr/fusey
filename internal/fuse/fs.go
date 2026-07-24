// Package fuse implements the FUSE filesystem layer for Fusey, translating
// Linux VFS calls into index and chunk store operations.
package fuse

import (
	"context"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/tofarr/fusey/internal/chunks"
	"github.com/tofarr/fusey/internal/index"
)

const (
	attrTTL  = 5 * time.Second
	entryTTL = 5 * time.Second
)

// FSIndex is the subset of *index.Index the FUSE layer needs. It is
// satisfied by both *index.Index (when journal recording is disabled) and
// *journal.JournaledIndex (when it is enabled). The FUSE layer holds an
// FSIndex so the main wiring code can choose which implementation to
// inject based on FUSEY_JOURNAL_ENABLED.
type FSIndex interface {
	GetInode(ino uint64) (index.Inode, bool)
	Lookup(parentIno uint64, name string) (uint64, bool)
	Readdir(ino uint64) ([]index.DirEntry, error)
	GetExtents(ino uint64) ([]index.Extent, bool)
	GetSymlink(ino uint64) (string, bool)
	GetXattrs(ino uint64) map[string]string
	GetParentIno(ino uint64) uint64
	BlockSz() int32
	UsedBytes() int64

	SetAttr(ino uint64, mode, uid, gid *uint32, size *int64, atime, mtime *int64, now int64) error
	CreateInode(fileType index.FileType, mode, uid, gid, rdev uint32, now int64) (uint64, error)
	AddDirEntry(parentIno uint64, name string, childIno uint64, now int64) error
	RemoveDirEntry(parentIno uint64, name string, now int64) error
	Rename(srcParent uint64, srcName string, dstParent uint64, dstName string, now int64) error
	WriteExtent(ino uint64, ext index.Extent, now int64) error
	SetSymlink(ino uint64, target string, now int64) error
	SetXattr(ino uint64, name, value string, now int64) error
	RemoveXattr(ino uint64, name string, now int64) error
	TouchAtime(ino uint64, now int64)
}

// Fusey is the top-level filesystem object shared by all nodes and file handles.
type Fusey struct {
	idx       FSIndex
	cs        *chunks.ChunkStore
	maxFSSize int64  // total capacity in bytes, reported via statfs
	cacheDir  string // directory where index snapshots are persisted on Fsync
	readOnly  bool   // when true, the FUSE mount is read-only (used by as-of mounts)
}

// New creates a Fusey filesystem.
// maxFSSize is FUSEY_MAX_SIZE; cacheDir is FUSEY_CACHE_DIR.
// When readOnly is true the underlying FUSE mount is opened with
// ReadOnly=true, which makes all write operations return EROFS at the
// kernel level. The idx is typically a *journal.JournaledIndex when
// journal recording is enabled, or *index.Index when it is not.
func New(idx FSIndex, cs *chunks.ChunkStore, maxFSSize int64, cacheDir string, readOnly bool) *Fusey {
	return &Fusey{idx: idx, cs: cs, maxFSSize: maxFSSize, cacheDir: cacheDir, readOnly: readOnly}
}

// IsReadOnly returns whether this filesystem was mounted read-only. Used
// by the FUSE layer to short-circuit write operations with EROFS before
// the kernel rejects them, producing better error messages.
func (f *Fusey) IsReadOnly() bool { return f.readOnly }

// Root returns the root node for mounting.
func (f *Fusey) Root() *Node {
	return &Node{ino: index.RootIno, f: f}
}

// --- Node ---

// Node represents any filesystem entry (file, directory, symlink, …).
// It is stateless: all data is kept in the index and chunk store.
type Node struct {
	fs.Inode
	ino uint64
	f   *Fusey
}

var _ fs.NodeGetattrer     = (*Node)(nil)
var _ fs.NodeSetattrer     = (*Node)(nil)
var _ fs.NodeLookuper      = (*Node)(nil)
var _ fs.NodeReaddirer     = (*Node)(nil)
var _ fs.NodeCreater       = (*Node)(nil)
var _ fs.NodeMkdirer       = (*Node)(nil)
var _ fs.NodeSymlinker     = (*Node)(nil)
var _ fs.NodeReadlinker    = (*Node)(nil)
var _ fs.NodeLinker        = (*Node)(nil)
var _ fs.NodeUnlinker      = (*Node)(nil)
var _ fs.NodeRmdirer       = (*Node)(nil)
var _ fs.NodeRenamer       = (*Node)(nil)
var _ fs.NodeOpener        = (*Node)(nil)
var _ fs.NodeStatfser      = (*Node)(nil)
var _ fs.NodeGetxattrer    = (*Node)(nil)
var _ fs.NodeSetxattrer    = (*Node)(nil)
var _ fs.NodeListxattrer   = (*Node)(nil)
var _ fs.NodeRemovexattrer = (*Node)(nil)

func (n *Node) Getattr(ctx context.Context, fh fs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	inode, ok := n.f.idx.GetInode(n.ino)
	if !ok {
		return syscall.ENOENT
	}
	fillAttr(&inode, &out.Attr)
	out.AttrValid = uint64(attrTTL.Seconds())
	return 0
}

func (n *Node) Setattr(ctx context.Context, fh fs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	now := time.Now().UnixNano()
	var mode, uid, gid *uint32
	var size *int64
	var atime, mtime *int64

	if in.Valid&gofuse.FATTR_MODE != 0 {
		m := in.Mode &^ uint32(index.Regular|index.Directory|index.Symlink|index.BlockDev|index.CharDev|index.Fifo|index.Socket)
		mode = &m
	}
	if in.Valid&gofuse.FATTR_UID != 0 {
		u := in.Uid
		uid = &u
	}
	if in.Valid&gofuse.FATTR_GID != 0 {
		g := in.Gid
		gid = &g
	}
	if in.Valid&gofuse.FATTR_SIZE != 0 {
		s := int64(in.Size)
		size = &s
	}
	if in.Valid&gofuse.FATTR_ATIME != 0 {
		at := int64(in.Atime)*1e9 + int64(in.Atimensec)
		atime = &at
	}
	if in.Valid&gofuse.FATTR_MTIME != 0 {
		mt := int64(in.Mtime)*1e9 + int64(in.Mtimensec)
		mtime = &mt
	}
	if err := n.f.idx.SetAttr(n.ino, mode, uid, gid, size, atime, mtime, now); err != nil {
		return syscall.EIO
	}
	inode, ok := n.f.idx.GetInode(n.ino)
	if !ok {
		return syscall.ENOENT
	}
	fillAttr(&inode, &out.Attr)
	out.AttrValid = uint64(attrTTL.Seconds())
	return 0
}

func (n *Node) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childIno, ok := n.f.idx.Lookup(n.ino, name)
	if !ok {
		return nil, syscall.ENOENT
	}
	childInode, ok := n.f.idx.GetInode(childIno)
	if !ok {
		return nil, syscall.ENOENT
	}
	fillAttr(&childInode, &out.Attr)
	out.EntryValid = uint64(entryTTL.Seconds())
	out.AttrValid = uint64(attrTTL.Seconds())

	child := n.NewInode(ctx, &Node{ino: childIno, f: n.f}, fs.StableAttr{
		Ino:  childIno,
		Mode: uint32(childInode.FileType),
	})
	return child, 0
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	inode, ok := n.f.idx.GetInode(n.ino)
	if !ok {
		return nil, syscall.ENOENT
	}
	if inode.FileType != index.Directory {
		return nil, syscall.ENOTDIR
	}
	entries, err := n.f.idx.Readdir(n.ino)
	if err != nil {
		return nil, syscall.EIO
	}
	parentIno := n.f.idx.GetParentIno(n.ino)
	list := []gofuse.DirEntry{
		{Mode: uint32(index.Directory), Name: ".", Ino: n.ino},
		{Mode: uint32(index.Directory), Name: "..", Ino: parentIno},
	}
	for _, e := range entries {
		childInode, ok := n.f.idx.GetInode(e.ChildIno)
		if !ok {
			continue
		}
		list = append(list, gofuse.DirEntry{
			Mode: uint32(childInode.FileType),
			Name: e.Name,
			Ino:  e.ChildIno,
		})
	}
	return fs.NewListDirStream(list), 0
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (
	*fs.Inode, fs.FileHandle, uint32, syscall.Errno,
) {
	now := time.Now().UnixNano()
	// Only keep permission bits from mode.
	perm := mode & 0o7777
	caller, ok := gofuse.FromContext(ctx)
	uid, gid := uint32(0), uint32(0)
	if ok {
		uid, gid = caller.Uid, caller.Gid
	}
	ino, err := n.f.idx.CreateInode(index.Regular, perm, uid, gid, 0, now)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	if err := n.f.idx.AddDirEntry(n.ino, name, ino, now); err != nil {
		return nil, nil, 0, syscall.EEXIST
	}
	inode, _ := n.f.idx.GetInode(ino)
	fillAttr(&inode, &out.Attr)
	out.EntryValid = uint64(entryTTL.Seconds())
	out.AttrValid = uint64(attrTTL.Seconds())

	child := n.NewInode(ctx, &Node{ino: ino, f: n.f}, fs.StableAttr{
		Ino:  ino,
		Mode: uint32(index.Regular),
	})
	fh := &FileHandle{ino: ino, f: n.f}
	return child, fh, 0, 0
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	now := time.Now().UnixNano()
	perm := mode & 0o7777
	caller, ok := gofuse.FromContext(ctx)
	uid, gid := uint32(0), uint32(0)
	if ok {
		uid, gid = caller.Uid, caller.Gid
	}
	ino, err := n.f.idx.CreateInode(index.Directory, perm, uid, gid, 0, now)
	if err != nil {
		return nil, syscall.EIO
	}
	if err := n.f.idx.AddDirEntry(n.ino, name, ino, now); err != nil {
		return nil, syscall.EEXIST
	}
	inode, _ := n.f.idx.GetInode(ino)
	fillAttr(&inode, &out.Attr)
	out.EntryValid = uint64(entryTTL.Seconds())
	out.AttrValid = uint64(attrTTL.Seconds())

	child := n.NewInode(ctx, &Node{ino: ino, f: n.f}, fs.StableAttr{
		Ino:  ino,
		Mode: uint32(index.Directory),
	})
	return child, 0
}

func (n *Node) Symlink(ctx context.Context, target, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	now := time.Now().UnixNano()
	caller, ok := gofuse.FromContext(ctx)
	uid, gid := uint32(0), uint32(0)
	if ok {
		uid, gid = caller.Uid, caller.Gid
	}
	ino, err := n.f.idx.CreateInode(index.Symlink, 0o777, uid, gid, 0, now)
	if err != nil {
		return nil, syscall.EIO
	}
	if err := n.f.idx.AddDirEntry(n.ino, name, ino, now); err != nil {
		return nil, syscall.EEXIST
	}
	if err := n.f.idx.SetSymlink(ino, target, now); err != nil {
		return nil, syscall.EIO
	}
	inode, _ := n.f.idx.GetInode(ino)
	fillAttr(&inode, &out.Attr)
	out.EntryValid = uint64(entryTTL.Seconds())
	out.AttrValid = uint64(attrTTL.Seconds())

	child := n.NewInode(ctx, &Node{ino: ino, f: n.f}, fs.StableAttr{
		Ino:  ino,
		Mode: uint32(index.Symlink),
	})
	return child, 0
}

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	target, ok := n.f.idx.GetSymlink(n.ino)
	if !ok {
		return nil, syscall.EINVAL
	}
	return []byte(target), 0
}

func (n *Node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *gofuse.EntryOut) (*fs.Inode, syscall.Errno) {
	now := time.Now().UnixNano()
	targetNode, ok := target.(*Node)
	if !ok {
		return nil, syscall.EINVAL
	}
	targetInode, ok := n.f.idx.GetInode(targetNode.ino)
	if !ok {
		return nil, syscall.ENOENT
	}
	if targetInode.FileType == index.Directory {
		return nil, syscall.EPERM
	}
	if err := n.f.idx.AddDirEntry(n.ino, name, targetNode.ino, now); err != nil {
		return nil, syscall.EEXIST
	}
	updatedInode, _ := n.f.idx.GetInode(targetNode.ino)
	fillAttr(&updatedInode, &out.Attr)
	out.EntryValid = uint64(entryTTL.Seconds())
	out.AttrValid = uint64(attrTTL.Seconds())

	child := n.NewInode(ctx, &Node{ino: targetNode.ino, f: n.f}, fs.StableAttr{
		Ino:  targetNode.ino,
		Mode: uint32(updatedInode.FileType),
	})
	return child, 0
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	now := time.Now().UnixNano()
	if err := n.f.idx.RemoveDirEntry(n.ino, name, now); err != nil {
		return syscall.ENOENT
	}
	return 0
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	now := time.Now().UnixNano()
	childIno, ok := n.f.idx.Lookup(n.ino, name)
	if !ok {
		return syscall.ENOENT
	}
	childInode, ok := n.f.idx.GetInode(childIno)
	if !ok {
		return syscall.ENOENT
	}
	if childInode.FileType != index.Directory {
		return syscall.ENOTDIR
	}
	if err := n.f.idx.RemoveDirEntry(n.ino, name, now); err != nil {
		if strings.Contains(err.Error(), "not empty") {
			return syscall.ENOTEMPTY
		}
		return syscall.EIO
	}
	return 0
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	now := time.Now().UnixNano()
	newParentNode, ok := newParent.(*Node)
	if !ok {
		return syscall.EINVAL
	}
	if err := n.f.idx.Rename(n.ino, name, newParentNode.ino, newName, now); err != nil {
		return syscall.EIO
	}
	return 0
}

func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if _, ok := n.f.idx.GetInode(n.ino); !ok {
		return nil, 0, syscall.ENOENT
	}
	return &FileHandle{ino: n.ino, f: n.f}, 0, 0
}

func (n *Node) Statfs(ctx context.Context, out *gofuse.StatfsOut) syscall.Errno {
	used := n.f.idx.UsedBytes()
	total := n.f.maxFSSize
	free := total - used
	if free < 0 {
		free = 0
	}
	bsize := uint64(n.f.idx.BlockSz())
	out.Bsize = uint32(bsize)
	out.Frsize = uint32(bsize)
	out.Blocks = uint64(total) / bsize
	out.Bfree = uint64(free) / bsize
	out.Bavail = out.Bfree
	// Report a generous inode count; we don't enforce a hard limit on inodes.
	out.Files = uint64(total) / 4096
	out.Ffree = out.Files
	out.NameLen = 255
	return 0
}

func (n *Node) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	attrs := n.f.idx.GetXattrs(n.ino)
	v, ok := attrs[attr]
	if !ok {
		return 0, syscall.ENODATA
	}
	if len(dest) == 0 {
		return uint32(len(v)), 0
	}
	if len(dest) < len(v) {
		return 0, syscall.ERANGE
	}
	return uint32(copy(dest, v)), 0
}

func (n *Node) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	now := time.Now().UnixNano()
	if err := n.f.idx.SetXattr(n.ino, attr, string(data), now); err != nil {
		return syscall.EIO
	}
	return 0
}

func (n *Node) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	attrs := n.f.idx.GetXattrs(n.ino)
	var buf []byte
	for k := range attrs {
		buf = append(buf, []byte(k)...)
		buf = append(buf, 0)
	}
	if len(dest) == 0 {
		return uint32(len(buf)), 0
	}
	if len(dest) < len(buf) {
		return 0, syscall.ERANGE
	}
	return uint32(copy(dest, buf)), 0
}

func (n *Node) Removexattr(ctx context.Context, attr string) syscall.Errno {
	now := time.Now().UnixNano()
	if err := n.f.idx.RemoveXattr(n.ino, attr, now); err != nil {
		return syscall.ENODATA
	}
	return 0
}

// --- FileHandle ---

// FileHandle is an open file descriptor. Reads and writes go through the
// index and chunk store.
type FileHandle struct {
	ino uint64
	f   *Fusey
}

var _ fs.FileReader  = (*FileHandle)(nil)
var _ fs.FileWriter  = (*FileHandle)(nil)
var _ fs.FileFlusher = (*FileHandle)(nil)
var _ fs.FileFsyncer = (*FileHandle)(nil)

func (fh *FileHandle) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	inode, ok := fh.f.idx.GetInode(fh.ino)
	if !ok {
		return nil, syscall.ENOENT
	}
	if off >= inode.Size {
		return gofuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > inode.Size {
		end = inode.Size
	}
	need := end - off

	exts, ok := fh.f.idx.GetExtents(fh.ino)
	if !ok {
		return gofuse.ReadResultData(nil), 0
	}

	buf := make([]byte, need)
	for _, e := range exts {
		eEnd := e.FileOffset + e.Length
		if eEnd <= off || e.FileOffset >= end {
			continue
		}
		// Intersection of [off, end) and [e.FileOffset, eEnd).
		readStart := max64(off, e.FileOffset)
		readEnd := min64(end, eEnd)
		chunkOffset := e.ChunkOffset + (readStart - e.FileOffset)
		data, err := fh.f.cs.Read(ctx, e.ChunkID, chunkOffset, readEnd-readStart)
		if err != nil {
			return nil, syscall.EIO
		}
		copy(buf[readStart-off:], data)
	}
	fh.f.idx.TouchAtime(fh.ino, time.Now().UnixNano())
	return gofuse.ReadResultData(buf), 0
}

func (fh *FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	now := time.Now().UnixNano()

	// ENOSPC: only charge for bytes that extend the file beyond its current end.
	// Overwrites at existing offsets don't change the logical size and are always
	// allowed, matching POSIX behaviour for filesystems near capacity.
	inode, ok := fh.f.idx.GetInode(fh.ino)
	if !ok {
		return 0, syscall.ENOENT
	}
	newEnd := off + int64(len(data))
	if newEnd > inode.Size {
		delta := newEnd - inode.Size
		if fh.f.idx.UsedBytes()+delta > fh.f.maxFSSize {
			return 0, syscall.ENOSPC
		}
	}

	ext, err := fh.f.cs.Append(ctx, data, off)
	if err != nil {
		return 0, syscall.EIO
	}
	if err := fh.f.idx.WriteExtent(fh.ino, ext, now); err != nil {
		return 0, syscall.EIO
	}
	return uint32(len(data)), 0
}

func (fh *FileHandle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

// Fsync makes the file's data durable:
//  1. Flush the in-memory active chunk to the backing store so all written
//     bytes are retrievable without relying on the process staying alive.
//  2. Persist the index snapshot to cacheDir so the file-to-extent mapping
//     survives a restart.
//
// Both operations are idempotent; calling Fsync multiple times is safe.
func (fh *FileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	if err := fh.f.cs.FlushActive(ctx); err != nil {
		return syscall.EIO
	}
	if fh.f.cacheDir != "" {
		// Save the underlying index. FUSE holds an FSIndex interface;
		// both *index.Index and *journal.JournaledIndex have the Save
		// method on the inner index, so we unwrap the wrapper if needed.
		var raw *index.Index
		if w, ok := fh.f.idx.(interface{ Inner() *index.Index }); ok {
			raw = w.Inner()
		} else {
			raw = fh.f.idx.(*index.Index)
		}
		if err := index.Save(raw, fh.f.cacheDir); err != nil {
			return syscall.EIO
		}
	}
	return 0
}

// --- helpers ---

func fillAttr(inode *index.Inode, attr *gofuse.Attr) {
	attr.Ino = inode.Ino
	attr.Size = uint64(inode.Size)
	attr.Blocks = uint64(inode.Blocks)
	attr.Atime = uint64(inode.Atime / 1e9)
	attr.Atimensec = uint32(inode.Atime % 1e9)
	attr.Mtime = uint64(inode.Mtime / 1e9)
	attr.Mtimensec = uint32(inode.Mtime % 1e9)
	attr.Ctime = uint64(inode.Ctime / 1e9)
	attr.Ctimensec = uint32(inode.Ctime % 1e9)
	attr.Mode = inode.FullMode()
	attr.Nlink = inode.Nlink
	attr.Uid = inode.UID
	attr.Gid = inode.GID
	attr.Rdev = inode.Rdev
	attr.Blksize = uint32(inode.Blksize)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
