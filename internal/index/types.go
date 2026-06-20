package index

import "syscall"

// FileType corresponds to the FileType variant in types.qnt.
// Values are the standard Linux file-type bits from the mode word.
type FileType uint32

const (
	Regular   FileType = syscall.S_IFREG
	Directory FileType = syscall.S_IFDIR
	Symlink   FileType = syscall.S_IFLNK
	BlockDev  FileType = syscall.S_IFBLK
	CharDev   FileType = syscall.S_IFCHR
	Fifo      FileType = syscall.S_IFIFO
	Socket    FileType = syscall.S_IFSOCK
)

// Inode holds the full POSIX metadata for a single filesystem object.
// It corresponds to the Inode type in types.qnt.
// Mode stores only the permission bits (lower 12 bits); FileType holds the
// upper bits. The full syscall mode is FileType | Mode.
type Inode struct {
	Ino      uint64   `json:"ino"`
	FileType FileType `json:"fileType"`
	Mode     uint32   `json:"mode"`    // permission bits only (0o0000–0o7777)
	Nlink    uint32   `json:"nlink"`
	UID      uint32   `json:"uid"`
	GID      uint32   `json:"gid"`
	Size     int64    `json:"size"`
	Atime    int64    `json:"atime"`   // nanoseconds since Unix epoch
	Mtime    int64    `json:"mtime"`
	Ctime    int64    `json:"ctime"`
	Blksize  int32    `json:"blksize"`
	Blocks   int64    `json:"blocks"`  // 512-byte blocks allocated
	Rdev     uint32   `json:"rdev"`    // encoded as (major<<8 | minor); 0 for non-devices
}

// FullMode returns the complete Linux mode word (file type bits | permission bits).
func (n *Inode) FullMode() uint32 {
	return uint32(n.FileType) | n.Mode
}

// Extent describes a contiguous run of a file's data within one chunk object.
// It corresponds to the Extent type in types.qnt.
type Extent struct {
	ChunkID     string `json:"chunkId"`
	ChunkOffset int64  `json:"chunkOffset"`
	Length      int64  `json:"length"`
	FileOffset  int64  `json:"fileOffset"`
}

// DirEntry is a single name-to-inode binding inside a directory.
// It corresponds to the DirEntry type in types.qnt.
type DirEntry struct {
	ParentIno uint64 `json:"parentIno"`
	Name      string `json:"name"`
	ChildIno  uint64 `json:"childIno"`
}

// Snapshot is the serialised form of the index written to disk.
// It corresponds to IndexSnapshot in types.qnt.
type Snapshot struct {
	Inodes         map[uint64]*Inode            `json:"inodes"`
	DirEntries     []DirEntry                   `json:"dirEntries"`
	ExtentMap      map[uint64][]Extent          `json:"extentMap"`
	SymlinkTargets map[uint64]string            `json:"symlinkTargets"`
	Xattrs         map[uint64]map[string]string `json:"xattrs"`
	NextIno        uint64                       `json:"nextIno"`
}

// blocks512 returns the number of 512-byte blocks needed to hold size bytes.
func blocks512(size int64) int64 {
	return (size + 511) / 512
}
