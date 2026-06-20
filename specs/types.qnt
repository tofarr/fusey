// types.qnt
//
// Shared type definitions for the Fusey filesystem.
//
// Fusey is a FUSE filesystem that stores file data as append-only chunk
// objects in an object store (e.g. S3), with an index that tracks inode
// metadata and the location of every extent of data within those chunks.
// This module defines the data types used across all other modules.

module types {

  // -------------------------------------------------------------------------
  // File types (the upper bits of a POSIX mode word)
  // -------------------------------------------------------------------------

  type FileType =
    | Regular      // normal data file
    | Directory    // directory (contains DirEntries)
    | Symlink      // symbolic link (has a target path string)
    | BlockDev     // block device (has rdev)
    | CharDev      // character device (has rdev)
    | Fifo         // named pipe
    | Socket       // Unix domain socket

  // -------------------------------------------------------------------------
  // Inode — full POSIX metadata for a single filesystem object.
  //
  // This is what gets stored in the index, both in memory and in the
  // on-disk cache.  Every field maps directly to the corresponding
  // struct stat field exposed to userspace via FUSE.
  // -------------------------------------------------------------------------

  type Inode = {
    ino     : int,    // inode number, unique within this filesystem instance
    fileType: FileType,
    mode    : int,    // permission bits: rwxrwxrwx + setuid/setgid/sticky (0..0o7777)
    nlink   : int,    // number of hard links pointing at this inode (>= 1 while live)
    uid     : int,    // owner user ID
    gid     : int,    // owner group ID
    size    : int,    // logical file size in bytes (0 for dirs / devices)
    atime   : int,    // last access time, nanoseconds since Unix epoch
    mtime   : int,    // last data-modification time, nanoseconds since epoch
    ctime   : int,    // last metadata-change time, nanoseconds since epoch
    blksize : int,    // preferred I/O block size reported to the kernel
    blocks  : int,    // number of 512-byte blocks currently allocated
    rdev    : int     // device ID encoded as (major << 8 | minor), 0 for non-devices
  }

  // -------------------------------------------------------------------------
  // Extent — a contiguous run of a file's data inside one chunk object.
  //
  // A file's data is stored as an ordered list of extents.  Each extent
  // records:
  //   - which chunk object the data lives in (chunkId)
  //   - where within that chunk it starts (chunkOffset)
  //   - how many bytes it covers (length)
  //   - where in the logical file those bytes belong (fileOffset)
  //
  // The extents for a file partition [0, inode.size) without gaps or overlaps.
  // -------------------------------------------------------------------------

  type Extent = {
    chunkId    : str,  // ID of the chunk object in the backing store
    chunkOffset: int,  // byte offset of this extent's data within the chunk
    length     : int,  // number of bytes (> 0)
    fileOffset : int   // corresponding byte offset within the logical file
  }

  // -------------------------------------------------------------------------
  // DirEntry — a single name-to-inode binding inside a directory.
  // -------------------------------------------------------------------------

  type DirEntry = {
    parentIno: int,  // inode number of the containing directory
    name     : str,  // filename component (no slashes, not "." or "..")
    childIno : int   // inode number of the referenced filesystem object
  }

  // -------------------------------------------------------------------------
  // ChunkStatus — lifecycle state of a chunk object.
  //
  // Active:  the chunk is currently being written to (append-only).
  //          Only one chunk is Active at any time.
  // Sealed:  the chunk is complete and immutable; may be referenced by
  //          index extents indefinitely, or freed after compaction.
  // -------------------------------------------------------------------------

  type ChunkStatus = Active | Sealed

  // -------------------------------------------------------------------------
  // Chunk — metadata about one chunk object in the backing store.
  // -------------------------------------------------------------------------

  type Chunk = {
    id    : str,         // unique chunk identifier (e.g. a UUID or sequential int)
    status: ChunkStatus,
    size  : int          // number of bytes written so far (final size once sealed)
  }

  // -------------------------------------------------------------------------
  // IndexSnapshot — the serialised form of the index written to the disk cache
  // and to the backing store.  Capturing all three maps in one record lets us
  // reason about atomic persistence.
  // -------------------------------------------------------------------------

  type IndexSnapshot = {
    inodes        : int -> Inode,
    dirEntries    : Set[DirEntry],
    extentMap     : int -> List[Extent],
    symlinkTargets: int -> str,
    xattrs        : int -> (str -> str),
    nextIno       : int
  }
}
