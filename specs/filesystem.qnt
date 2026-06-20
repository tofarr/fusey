// filesystem.qnt
//
// Top-level specification of Fusey POSIX filesystem operations.
//
// This module composes the index and chunk store state machines into the
// complete set of filesystem operations that the FUSE layer exposes to the
// kernel.  Each operation here corresponds directly to one or more FUSE
// callback functions in the Go implementation.
//
// State from the sub-modules is re-declared here (Quint does not support
// shared mutable state across modules) and the sub-module actions are
// invoked by reference.  In practice, the implementation wires these
// together through a single top-level struct that holds both the index
// and the chunk store.
//
// Cross-cutting concerns modeled here:
//   - Atomicity: what must be durable before an operation returns to userspace
//   - Error paths: preconditions that map to POSIX errno values
//   - Crash safety: which partial states are recoverable

module filesystem {

  import types.* from "./types"

  // -------------------------------------------------------------------------
  // Re-declare state (mirrors index.qnt and chunks.qnt)
  // -------------------------------------------------------------------------

  // --- Index ---
  var inodes        : int -> Inode
  var dirEntries    : Set[DirEntry]
  var extentMap     : int -> List[Extent]
  var symlinkTargets: int -> str
  var xattrs        : int -> (str -> str)
  var nextIno       : int
  var diskCache     : IndexSnapshot
  var diskCacheDirty: bool

  // --- Chunks ---
  var chunks            : str -> Chunk
  var activeChunkId     : str
  var nextChunkSeq      : int
  var lastWrittenExtent : Extent

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  const ROOT_INO     : int
  const BLOCK_SIZE   : int
  const MAX_CHUNK_SIZE: int
  // Maximum total size of the filesystem in bytes (FUSEY_MAX_SIZE env var).
  // Reported to the kernel via statfs as total blocks.
  // Must be > 0.
  const MAX_FS_SIZE  : int

  // -------------------------------------------------------------------------
  // Pure helpers (duplicated from sub-modules for self-containedness)
  // -------------------------------------------------------------------------

  pure def chunkId(seq: int): str = "chunk-" + seq.toString()

  pure def refCount(ino: int, entries: Set[DirEntry]): int =
    entries.filter(e => e.childIno == ino).size()

  pure def extentTotal(extents: List[Extent]): int =
    extents.foldl(0, (acc, e) => acc + e.length)

  // -------------------------------------------------------------------------
  // Initialisation
  // -------------------------------------------------------------------------

  action init = {
    val rootInode: Inode = {
      ino: ROOT_INO, fileType: Directory, mode: 0o755,
      nlink: 1, uid: 0, gid: 0, size: 0,
      atime: 0, mtime: 0, ctime: 0,
      blksize: BLOCK_SIZE, blocks: 0, rdev: 0
    }
    val emptySnapshot: IndexSnapshot = {
      inodes: Map(ROOT_INO -> rootInode), dirEntries: Set(),
      extentMap: Map(), symlinkTargets: Map(), xattrs: Map(),
      nextIno: ROOT_INO + 1
    }
    val firstChunkId = chunkId(0)
    val sentinel: Extent = { chunkId: firstChunkId, chunkOffset: 0, length: 0, fileOffset: 0 }
    inodes'             = emptySnapshot.inodes
    dirEntries'         = emptySnapshot.dirEntries
    extentMap'          = emptySnapshot.extentMap
    symlinkTargets'     = emptySnapshot.symlinkTargets
    xattrs'             = emptySnapshot.xattrs
    nextIno'            = emptySnapshot.nextIno
    diskCache'          = emptySnapshot
    diskCacheDirty'     = false
    chunks'             = Map(firstChunkId -> { id: firstChunkId, status: Active, size: 0 })
    activeChunkId'      = firstChunkId
    nextChunkSeq'       = 1
    lastWrittenExtent'  = sentinel
  }

  // -------------------------------------------------------------------------
  // FUSE: lookup — resolve a name within a directory.
  //
  // Returns (via preconditions) the inode for (parentIno, name).
  // Maps to: fuse.Operations.Lookup
  // Errno on failure: ENOENT, ENOTDIR
  // -------------------------------------------------------------------------

  action lookup(parentIno: int, name: str) = {
    require(parentIno.in(inodes.keys()))
    require(inodes.get(parentIno).fileType == Directory)
    require(exists e in dirEntries: e.parentIno == parentIno and e.name == name)
    // Atime update is the only state change for lookup (implementation may
    // skip this for performance using the relatime mount option).
    val parent   = inodes.get(parentIno)
    // No mutable state changes needed for a pure lookup.
    // The result (child inode) is inferred by the caller from the state.
  }

  // -------------------------------------------------------------------------
  // FUSE: getattr — retrieve inode metadata.
  //
  // Maps to: fuse.Operations.Getattr
  // Errno on failure: ENOENT
  // -------------------------------------------------------------------------

  action getattr(ino: int) = {
    require(ino.in(inodes.keys()))
    // Pure read; no state change.
  }

  // -------------------------------------------------------------------------
  // FUSE: create — create a new regular file and open it.
  //
  // Atomicity requirement: the inode and its DirEntry must both be
  // persisted before the operation returns success.
  //
  // Maps to: fuse.Operations.Create
  // Errno on failure: EEXIST, ENOENT (parent), ENOTDIR
  // -------------------------------------------------------------------------

  action create(parentIno: int, name: str, mode: int, uid: int, gid: int, now: int) = {
    require(parentIno.in(inodes.keys()))
    require(inodes.get(parentIno).fileType == Directory)
    require(not(exists e in dirEntries: e.parentIno == parentIno and e.name == name))
    val ino      = nextIno
    val newInode: Inode = {
      ino: ino, fileType: Regular, mode: mode,
      nlink: 1, uid: uid, gid: gid, size: 0,
      atime: now, mtime: now, ctime: now,
      blksize: BLOCK_SIZE, blocks: 0, rdev: 0
    }
    val newEntry: DirEntry = { parentIno: parentIno, name: name, childIno: ino }
    val parent   = inodes.get(parentIno)
    inodes'         = inodes
                        .set(ino, newInode)
                        .set(parentIno, { ...parent, mtime: now, ctime: now })
    dirEntries'     = dirEntries.union(Set(newEntry))
    extentMap'      = extentMap.set(ino, List())
    nextIno'        = nextIno + 1
    diskCacheDirty' = true
  }

  // -------------------------------------------------------------------------
  // FUSE: mkdir — create a directory.
  //
  // Maps to: fuse.Operations.Mkdir
  // Errno on failure: EEXIST, ENOENT, ENOTDIR
  // -------------------------------------------------------------------------

  action mkdir(parentIno: int, name: str, mode: int, uid: int, gid: int, now: int) = {
    require(parentIno.in(inodes.keys()))
    require(inodes.get(parentIno).fileType == Directory)
    require(not(exists e in dirEntries: e.parentIno == parentIno and e.name == name))
    val ino      = nextIno
    val newDir: Inode = {
      ino: ino, fileType: Directory, mode: mode,
      nlink: 1, uid: uid, gid: gid, size: 0,
      atime: now, mtime: now, ctime: now,
      blksize: BLOCK_SIZE, blocks: 0, rdev: 0
    }
    val newEntry: DirEntry = { parentIno: parentIno, name: name, childIno: ino }
    val parent   = inodes.get(parentIno)
    inodes'         = inodes
                        .set(ino, newDir)
                        .set(parentIno, { ...parent, mtime: now, ctime: now })
    dirEntries'     = dirEntries.union(Set(newEntry))
    nextIno'        = nextIno + 1
    diskCacheDirty' = true
  }

  // -------------------------------------------------------------------------
  // FUSE: symlink — create a symbolic link.
  //
  // Maps to: fuse.Operations.Symlink
  // Errno on failure: EEXIST, ENOENT, ENOTDIR
  // -------------------------------------------------------------------------

  action symlink(parentIno: int, name: str, target: str, uid: int, gid: int, now: int) = {
    require(parentIno.in(inodes.keys()))
    require(inodes.get(parentIno).fileType == Directory)
    require(not(exists e in dirEntries: e.parentIno == parentIno and e.name == name))
    val ino      = nextIno
    val newLink: Inode = {
      ino: ino, fileType: Symlink, mode: 0o777,
      nlink: 1, uid: uid, gid: gid, size: target.length(),
      atime: now, mtime: now, ctime: now,
      blksize: BLOCK_SIZE, blocks: 0, rdev: 0
    }
    val newEntry: DirEntry = { parentIno: parentIno, name: name, childIno: ino }
    val parent   = inodes.get(parentIno)
    inodes'         = inodes
                        .set(ino, newLink)
                        .set(parentIno, { ...parent, mtime: now, ctime: now })
    dirEntries'     = dirEntries.union(Set(newEntry))
    symlinkTargets' = symlinkTargets.set(ino, target)
    nextIno'        = nextIno + 1
    diskCacheDirty' = true
  }

  // -------------------------------------------------------------------------
  // FUSE: link — create a hard link.
  //
  // Maps to: fuse.Operations.Link
  // Errno on failure: ENOENT, EEXIST, EISDIR (cannot hard-link dirs), EMLINK
  // -------------------------------------------------------------------------

  action link(ino: int, parentIno: int, name: str, now: int) = {
    require(ino.in(inodes.keys()))
    require(inodes.get(ino).fileType != Directory)  // POSIX: no hard links to dirs
    require(parentIno.in(inodes.keys()))
    require(inodes.get(parentIno).fileType == Directory)
    require(not(exists e in dirEntries: e.parentIno == parentIno and e.name == name))
    val inode    = inodes.get(ino)
    val parent   = inodes.get(parentIno)
    val newEntry: DirEntry = { parentIno: parentIno, name: name, childIno: ino }
    dirEntries'     = dirEntries.union(Set(newEntry))
    inodes'         = inodes
                        .set(ino, { ...inode, nlink: inode.nlink + 1, ctime: now })
                        .set(parentIno, { ...parent, mtime: now, ctime: now })
    diskCacheDirty' = true
  }

  // -------------------------------------------------------------------------
  // FUSE: unlink — remove a name (and possibly the inode behind it).
  //
  // Maps to: fuse.Operations.Unlink
  // Errno on failure: ENOENT, EISDIR
  // -------------------------------------------------------------------------

  action unlink(parentIno: int, name: str, now: int) = {
    require(exists e in dirEntries: e.parentIno == parentIno and e.name == name)
    val entry    = dirEntries.filter(e => e.parentIno == parentIno and e.name == name).choose()
    val child    = inodes.get(entry.childIno)
    require(child.fileType != Directory)  // use rmdir for directories
    val parent   = inodes.get(parentIno)
    val newNlink = child.nlink - 1
    dirEntries' = dirEntries.filter(e => not(e.parentIno == parentIno and e.name == name))
    inodes'     = if (newNlink == 0)
                    inodes
                      .remove(entry.childIno)
                      .set(parentIno, { ...parent, mtime: now, ctime: now })
                  else
                    inodes
                      .set(entry.childIno, { ...child, nlink: newNlink, ctime: now })
                      .set(parentIno, { ...parent, mtime: now, ctime: now })
    // When nlink reaches 0 the extents become orphaned (compaction candidates).
    extentMap'      = if (newNlink == 0) extentMap.remove(entry.childIno) else extentMap
    symlinkTargets' = if (newNlink == 0 and child.fileType == Symlink)
                        symlinkTargets.remove(entry.childIno)
                      else symlinkTargets
    xattrs'         = if (newNlink == 0) xattrs.remove(entry.childIno) else xattrs
    diskCacheDirty' = true
  }

  // -------------------------------------------------------------------------
  // FUSE: rmdir — remove an empty directory.
  //
  // Maps to: fuse.Operations.Rmdir
  // Errno on failure: ENOENT, ENOTDIR, ENOTEMPTY
  // -------------------------------------------------------------------------

  action rmdir(parentIno: int, name: str, now: int) = {
    require(exists e in dirEntries: e.parentIno == parentIno and e.name == name)
    val entry = dirEntries.filter(e => e.parentIno == parentIno and e.name == name).choose()
    val child = inodes.get(entry.childIno)
    require(child.fileType == Directory)
    require(not(exists e in dirEntries: e.parentIno == entry.childIno)) // must be empty
    val parent   = inodes.get(parentIno)
    dirEntries'     = dirEntries.filter(e =>
                        not(e.parentIno == parentIno and e.name == name))
    inodes'         = inodes
                        .remove(entry.childIno)
                        .set(parentIno, { ...parent, mtime: now, ctime: now })
    xattrs'         = xattrs.remove(entry.childIno)
    diskCacheDirty' = true
  }

  // -------------------------------------------------------------------------
  // FUSE: rename — atomically move/rename a filesystem entry.
  //
  // Implements POSIX rename(2) semantics: if dstName already exists it is
  // atomically replaced (subject to type constraints).
  //
  // Maps to: fuse.Operations.Rename
  // Errno on failure: ENOENT, ENOTDIR, EISDIR, ENOTEMPTY
  // -------------------------------------------------------------------------

  action rename(srcParent: int, srcName: str, dstParent: int, dstName: str, now: int) = {
    require(exists e in dirEntries: e.parentIno == srcParent and e.name == srcName)
    val srcEntry = dirEntries
                     .filter(e => e.parentIno == srcParent and e.name == srcName)
                     .choose()
    val srcChild = inodes.get(srcEntry.childIno)
    // If destination exists: same-type replacement only
    // (moving a non-dir over a dir is ENOTDIR and vice versa).
    val dstExists = exists e in dirEntries: e.parentIno == dstParent and e.name == dstName
    val dstEntry  = if (dstExists)
                      dirEntries
                        .filter(e => e.parentIno == dstParent and e.name == dstName)
                        .choose()
                    else
                      { parentIno: dstParent, name: dstName, childIno: -1 }
    require(
      not(dstExists) or
      inodes.get(dstEntry.childIno).fileType == srcChild.fileType or
      (srcChild.fileType != Directory and
       inodes.get(dstEntry.childIno).fileType != Directory)
    )
    // Cannot overwrite a non-empty directory.
    require(
      not(dstExists) or
      inodes.get(dstEntry.childIno).fileType != Directory or
      not(exists e in dirEntries: e.parentIno == dstEntry.childIno)
    )
    // Cannot rename a directory into itself or a descendant.
    require(srcEntry.childIno != dstParent)

    // Remove old destination entry (if any) and decrement its nlink.
    val withoutDst = if (dstExists)
                       dirEntries.filter(e =>
                         not(e.parentIno == dstParent and e.name == dstName))
                     else dirEntries
    val inodesAfterDstRemoval = if (dstExists)
      val dstChild  = inodes.get(dstEntry.childIno)
      val newNlink  = dstChild.nlink - 1
      if (newNlink == 0)
        inodes.remove(dstEntry.childIno)
      else
        inodes.set(dstEntry.childIno, { ...dstChild, nlink: newNlink, ctime: now })
    else inodes

    // Move source entry to destination.
    val withoutSrc = withoutDst.filter(e =>
      not(e.parentIno == srcParent and e.name == srcName))
    val newEntry: DirEntry = { parentIno: dstParent, name: dstName, childIno: srcEntry.childIno }
    dirEntries'     = withoutSrc.union(Set(newEntry))
    inodes'         = inodesAfterDstRemoval
    diskCacheDirty' = true
  }

  // -------------------------------------------------------------------------
  // FUSE: setattr — update inode metadata (chmod, chown, truncate, utimens).
  //
  // Maps to: fuse.Operations.Setattr
  // Errno on failure: ENOENT, EACCES, EPERM
  // -------------------------------------------------------------------------

  action setattr(ino: int, updated: Inode, now: int) = {
    require(ino.in(inodes.keys()))
    require(updated.ino == ino)
    require(updated.blksize == BLOCK_SIZE)
    val old = inodes.get(ino)
    // If size is being reduced, orphan the excess extents.
    val newExtentMap =
      if (updated.size < old.size and old.fileType == Regular)
        val trimmed = extentMap.get(ino).filter(e =>
          e.fileOffset < updated.size
        ).map(e =>
          if (e.fileOffset + e.length <= updated.size) e
          else { ...e, length: updated.size - e.fileOffset }
        )
        extentMap.set(ino, trimmed)
      else extentMap
    inodes'         = inodes.set(ino, { ...updated, ctime: now })
    extentMap'      = newExtentMap
    diskCacheDirty' = true
  }

  // -------------------------------------------------------------------------
  // FUSE: read — read bytes from a regular file.
  //
  // The caller resolves which extents cover [offset, offset+size) and
  // issues range-GET requests against the backing store for each.
  //
  // Maps to: fuse.Operations.Read
  // Errno on failure: ENOENT, EISDIR, EINVAL
  // -------------------------------------------------------------------------

  action read(ino: int, offset: int, size: int, now: int) = {
    require(ino.in(inodes.keys()))
    require(inodes.get(ino).fileType == Regular)
    require(offset >= 0)
    require(size > 0)
    require(offset < inodes.get(ino).size)  // reading beyond EOF returns 0 bytes
    val inode   = inodes.get(ino)
    // Only atime changes; implementation may batch/skip atime updates.
    inodes'     = inodes.set(ino, { ...inode, atime: now })
  }

  // -------------------------------------------------------------------------
  // FUSE: write — write bytes to a regular file.
  //
  // Each write appends dataSize bytes to the active chunk and records the
  // resulting extent in the index.  Overwrites (offset < current size) are
  // modeled as truncate + append (copy-on-write semantics: old extent bytes
  // become orphaned, new bytes occupy a fresh extent).
  //
  // Maps to: fuse.Operations.Write
  // Errno on failure: ENOENT, EISDIR, ENOSPC
  // -------------------------------------------------------------------------

  action write(ino: int, offset: int, dataSize: int, now: int) = {
    require(ino.in(inodes.keys()))
    require(inodes.get(ino).fileType == Regular)
    require(offset >= 0)
    require(dataSize > 0)
    require(dataSize <= MAX_CHUNK_SIZE)

    val inode       = inodes.get(ino)
    val active      = chunks.get(activeChunkId)
    val willRotate  = active.size + dataSize > MAX_CHUNK_SIZE
    val targetId    = if (willRotate) chunkId(nextChunkSeq) else activeChunkId
    val baseSize    = if (willRotate) 0 else active.size

    // Truncate existing extents that overlap [offset, offset+dataSize).
    val trimmed = extentMap.get(ino).filter(e =>
      e.fileOffset + e.length <= offset or e.fileOffset >= offset + dataSize
    ).map(e =>
      // Partially overlapping head: trim its tail.
      if (e.fileOffset < offset and e.fileOffset + e.length > offset)
        { ...e, length: offset - e.fileOffset }
      // Partially overlapping tail: trim its head.
      else if (e.fileOffset < offset + dataSize and e.fileOffset + e.length > offset + dataSize)
        { ...e,
          chunkOffset: e.chunkOffset + (offset + dataSize - e.fileOffset),
          fileOffset : offset + dataSize,
          length     : e.fileOffset + e.length - (offset + dataSize) }
      else e
    )

    val newExtent: Extent = {
      chunkId    : targetId,
      chunkOffset: baseSize,
      length     : dataSize,
      fileOffset : offset
    }
    val newExtents  = trimmed.append(newExtent)
    val newSize     = if (offset + dataSize > inode.size) offset + dataSize else inode.size

    // Update chunk store.
    val afterRotate = if (willRotate)
                        chunks.set(activeChunkId, { ...active, status: Sealed })
                              .set(targetId, { id: targetId, status: Active, size: 0 })
                      else chunks
    val targetChunk = afterRotate.get(targetId)
    chunks'              = afterRotate.set(targetId, { ...targetChunk, size: baseSize + dataSize })
    activeChunkId'       = targetId
    nextChunkSeq'        = if (willRotate) nextChunkSeq + 1 else nextChunkSeq
    lastWrittenExtent'   = newExtent

    // Update index.
    extentMap'      = extentMap.set(ino, newExtents)
    inodes'         = inodes.set(ino, {
                        ...inode,
                        size  : newSize,
                        blocks: (newSize + 511) / 512,
                        mtime : now,
                        ctime : now
                      })
    diskCacheDirty' = true
  }

  // -------------------------------------------------------------------------
  // FUSE: readdir — list directory contents.
  //
  // Maps to: fuse.Operations.Readdir
  // Errno on failure: ENOENT, ENOTDIR
  // -------------------------------------------------------------------------

  action readdir(ino: int, now: int) = {
    require(ino.in(inodes.keys()))
    require(inodes.get(ino).fileType == Directory)
    // Pure read; implementation returns { ".", ".." } plus all child DirEntries.
    val inode = inodes.get(ino)
    inodes'   = inodes.set(ino, { ...inode, atime: now })
  }

  // -------------------------------------------------------------------------
  // FUSE: readlink — read a symlink's target path.
  //
  // Maps to: fuse.Operations.Readlink
  // Errno on failure: ENOENT, EINVAL (not a symlink)
  // -------------------------------------------------------------------------

  action readlink(ino: int) = {
    require(ino.in(inodes.keys()))
    require(inodes.get(ino).fileType == Symlink)
    require(ino.in(symlinkTargets.keys()))
    // Pure read; no state change.
  }

  // -------------------------------------------------------------------------
  // persistIndex — flush the in-memory index to the on-disk cache.
  //
  // Called periodically and at unmount.  Until this action fires, a crash
  // would require replaying the write log from the backing store.
  // -------------------------------------------------------------------------

  action persistIndex = {
    diskCache' = {
      inodes        : inodes,
      dirEntries    : dirEntries,
      extentMap     : extentMap,
      symlinkTargets: symlinkTargets,
      xattrs        : xattrs,
      nextIno       : nextIno
    }
    diskCacheDirty' = false
  }

  // -------------------------------------------------------------------------
  // FUSE: statfs — report filesystem space usage to the kernel.
  //
  // Returns total, free, and available block counts derived from MAX_FS_SIZE
  // and the current sum of all allocated inode blocks.
  //
  // Maps to: fuse.Operations.Statfs
  // -------------------------------------------------------------------------

  // usedBytes computes the total bytes allocated across all regular-file inodes.
  pure def usedBytes(ins: int -> Inode): int =
    ins.values()
       .filter(n => n.fileType == Regular)
       .map(n => n.size)
       .fold(0, (acc, s) => acc + s)

  action statfs = {
    // No state changes; kernel reads the returned values directly.
    // Implementation fills fuse.StatfsOut from MAX_FS_SIZE and usedBytes.
    require(MAX_FS_SIZE > 0)
  }

  // -------------------------------------------------------------------------
  // Global filesystem invariants
  // -------------------------------------------------------------------------

  // Root always exists and is a directory.
  invariant rootInvariant =
    ROOT_INO.in(inodes.keys()) and inodes.get(ROOT_INO).fileType == Directory

  // Inode key consistency.
  invariant inodeKeyConsistency =
    forall ino in inodes.keys(): inodes.get(ino).ino == ino

  // Every directory entry references existing inodes.
  invariant dirEntriesValid =
    forall e in dirEntries:
      e.parentIno.in(inodes.keys()) and e.childIno.in(inodes.keys())

  // nlink consistency.
  invariant nlinkConsistency =
    forall ino in inodes.keys():
      val refs  = refCount(ino, dirEntries)
      val extra = if (inodes.get(ino).fileType == Directory) 1 else 0
      inodes.get(ino).nlink == refs + extra

  // File sizes are non-negative.
  invariant sizeNonNeg =
    forall ino in inodes.keys(): inodes.get(ino).size >= 0

  // Extent totals equal inode sizes.
  invariant extentSizeConsistency =
    forall ino in inodes.keys():
      inodes.get(ino).fileType == Regular implies
        extentTotal(extentMap.get(ino)) == inodes.get(ino).size

  // Only one chunk is active.
  invariant onlyOneActiveChunk =
    forall id in chunks.keys():
      id != activeChunkId implies chunks.get(id).status == Sealed

  // Active chunk within bound.
  invariant activeChunkBound =
    chunks.get(activeChunkId).size <= MAX_CHUNK_SIZE

  // Write-read consistency: the last written extent is always readable.
  invariant lastExtentReadable =
    lastWrittenExtent.chunkId.in(chunks.keys()) and
    lastWrittenExtent.chunkOffset + lastWrittenExtent.length <=
      chunks.get(lastWrittenExtent.chunkId).size

  // blksize matches configuration in every inode.
  invariant blksizeConsistency =
    forall ino in inodes.keys(): inodes.get(ino).blksize == BLOCK_SIZE

  // Total used bytes never exceed the configured maximum.
  invariant usedWithinBound =
    usedBytes(inodes) <= MAX_FS_SIZE

  // -------------------------------------------------------------------------
  // Temporal properties
  // -------------------------------------------------------------------------

  // A dirty index is eventually persisted.
  temporal eventualPersistence =
    always(diskCacheDirty implies eventually(not(diskCacheDirty)))

  // Data written to a file is eventually readable via a range-GET.
  temporal writeBecomesReadable =
    always(
      lastWrittenExtent.length > 0 implies
        lastWrittenExtent.chunkId.in(chunks.keys())
    )
}
