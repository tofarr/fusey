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

  // -------------------------------------------------------------------------
  // JournalEvent — a single recorded edit in the optional edit log.
  //
  // The journal is an append-only, sharded log of every state-mutating
  // operation on the index (with the deliberate exception of TouchAtime:
  // read access is too high-frequency and has no replay value).
  //
  // Each event carries a wall-clock nanosecond timestamp captured at the
  // point of mutation.  Combined with the monotonic per-event sequence
  // number assigned at append time, this supports both timestamp-based
  // (--as-of=<ts>) and sequence-based replay.
  //
  // Delete operations are first-class events (DeleteInode, RemoveDirEntry,
  // RemoveXattr) — the journal is not just an overwriting of the index file,
  // it can record tombstones that the index snapshot cannot.
  // -------------------------------------------------------------------------

  type CreateInodeRec = {
    ino     : int, fileType: FileType, mode: int, nlink: int,
    uid     : int, gid: int, rdev: int,
    atime   : int, mtime: int, ctime: int,
    blksize : int, blocks: int
  }

  // SetAttrRec captures the full post-mutation Inode fields.  SetAttr covers
  // chmod, chown, utimens, AND truncate (the size field changes are handled
  // by the same SetAttr op in the index), so a single event type is enough.
  // Replay re-applies the same SetAttr, which performs the same trim-or-grow
  // logic the live index would have used.
  type SetAttrRec = {
    ino   : int, mode: int, uid: int, gid: int, size: int,
    atime : int, mtime: int, ctime: int, nlink: int, rdev: int
  }

  type AddDirEntryRec    = { parentIno: int, name: str, childIno: int }
  type RemoveDirEntryRec = { parentIno: int, name: str }
  type RenameRec         = { srcParent: int, srcName: str, dstParent: int, dstName: str }
  type WriteExtentRec    = { ino: int, extent: Extent }
  type SetSymlinkRec     = { ino: int, target: str }
  type SetXattrRec       = { ino: int, name: str, value: str }
  type RemoveXattrRec    = { ino: int, name: str }
  type DeleteInodeRec    = { ino: int }

  type JournalEvent =
    | CreateInode   (CreateInodeRec)
    | DeleteInode   (DeleteInodeRec)
    | AddDirEntry   (AddDirEntryRec)
    | RemoveDirEntry(RemoveDirEntryRec)
    | Rename        (RenameRec)
    | SetAttr       (SetAttrRec)
    | WriteExtent   (WriteExtentRec)
    | SetSymlink    (SetSymlinkRec)
    | SetXattr      (SetXattrRec)
    | RemoveXattr   (RemoveXattrRec)

  // A timestamped, sequenced journal entry as it lives in the log.
  // seq is a per-daemon monotonically increasing counter assigned at append time.
  // ts is the wall-clock time in nanoseconds since the Unix epoch.
  type JournalEntry = {
    seq: int,
    ts : int,
    op : JournalEvent
  }
}
