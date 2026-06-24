// index.qnt
//
// Formal specification of the Fusey index state machine.
//
// The index is the authoritative record of:
//   - every inode and its POSIX metadata
//   - the directory tree (parent→name→child bindings)
//   - the extent map (which chunk bytes store each file's data)
//   - symlink targets
//   - extended attributes
//
// The index lives in memory at runtime and is persisted to two places:
//   1. An on-disk cache (local filesystem path, fast recovery)
//   2. The backing object store (durable, used when the disk cache is absent)
//
// The on-disk cache is modeled here as a shadow copy of the in-memory
// state.  Persistence is an explicit action so we can state invariants
// about the gap between memory and disk.

module index {

  import types.* from "./types"

  // -------------------------------------------------------------------------
  // Constants (resolved at startup from environment / configuration)
  // -------------------------------------------------------------------------

  // Inode number reserved for the filesystem root directory.
  const ROOT_INO: int

  // Preferred I/O block size reported to the kernel (FUSEY_BLOCK_SIZE env var).
  // Also used as the blksize field in newly-created inodes.
  const BLOCK_SIZE: int

  // -------------------------------------------------------------------------
  // State
  // -------------------------------------------------------------------------

  // Primary in-memory index.
  var inodes        : int -> Inode
  var dirEntries    : Set[DirEntry]
  var extentMap     : int -> List[Extent]
  var symlinkTargets: int -> str
  var xattrs        : int -> (str -> str)

  // Monotonically increasing counter; the next inode number to allocate.
  var nextIno: int

  // Shadow of the index as it was last flushed to the on-disk cache.
  // When diskCacheDirty is true the in-memory state has diverged from disk.
  var diskCache     : IndexSnapshot
  var diskCacheDirty: bool

  // Shadow of the index as it was last written to the remote object store.
  // remoteStoreDirty is set only by structural mutations (creates, deletes,
  // renames, writes, xattrs). TouchAtime (read-access) does not set it, so
  // atime-only updates do not trigger unnecessary presigned-URL round-trips.
  var remoteCache     : IndexSnapshot
  var remoteStoreDirty: bool

  // -------------------------------------------------------------------------
  // Pure helpers
  // -------------------------------------------------------------------------

  // Compare two inode maps structurally, ignoring the atime field.
  // TouchAtime updates atime without setting remoteStoreDirty, so the remote
  // cache may hold stale atimes while all other inode fields remain in sync.
  pure def inodesStructurallyEqual(a: int -> Inode, b: int -> Inode): bool =
    a.keys() == b.keys() and
    forall ino in a.keys():
      val ai = a.get(ino)
      val bi = b.get(ino)
      ai.ino      == bi.ino      and
      ai.fileType == bi.fileType and
      ai.mode     == bi.mode     and
      ai.nlink    == bi.nlink    and
      ai.uid      == bi.uid      and
      ai.gid      == bi.gid      and
      ai.size     == bi.size     and
      ai.mtime    == bi.mtime    and
      ai.ctime    == bi.ctime    and
      ai.blksize  == bi.blksize  and
      ai.blocks   == bi.blocks   and
      ai.rdev     == bi.rdev

  // Count how many DirEntries point to a given inode.
  pure def refCount(ino: int, entries: Set[DirEntry]): int =
    entries.filter(e => e.childIno == ino).size()

  // Sum the lengths of all extents for an inode.
  pure def extentTotal(extents: List[Extent]): int =
    extents.foldl(0, (acc, e) => acc + e.length)

  // True when two extents in the same file do not overlap.
  pure def noOverlap(a: Extent, b: Extent): bool =
    a.fileOffset + a.length <= b.fileOffset or
    b.fileOffset + b.length <= a.fileOffset

  // True when all pairs of extents in a list are non-overlapping.
  pure def extentsDisjoint(exts: List[Extent]): bool =
    forall i in exts.indices():
      forall j in exts.indices():
        i != j implies noOverlap(exts.nth(i), exts.nth(j))

  // -------------------------------------------------------------------------
  // Invariants
  // -------------------------------------------------------------------------

  // Every inode's ino field matches its position in the map.
  invariant inodeKeyConsistency =
    forall ino in inodes.keys(): inodes.get(ino).ino == ino

  // The root inode always exists and is a directory.
  invariant rootExists     = ROOT_INO.in(inodes.keys())
  invariant rootIsDir      = inodes.get(ROOT_INO).fileType == Directory

  // Every DirEntry references existing inodes for both parent and child.
  invariant dirEntriesValid =
    forall e in dirEntries:
      e.parentIno.in(inodes.keys()) and e.childIno.in(inodes.keys())

  // Parent of every DirEntry must be a directory.
  invariant parentIsDir =
    forall e in dirEntries:
      inodes.get(e.parentIno).fileType == Directory

  // No two entries in the same directory share a name.
  invariant namesUnique =
    forall e1 in dirEntries:
      forall e2 in dirEntries:
        (e1.parentIno == e2.parentIno and e1.name == e2.name) implies e1 == e2

  // nlink equals the number of DirEntries pointing at the inode.
  // Directories additionally count themselves (they are their own ".").
  invariant nlinkConsistency =
    forall ino in inodes.keys():
      val refs  = refCount(ino, dirEntries)
      val extra = if (inodes.get(ino).fileType == Directory) 1 else 0
      inodes.get(ino).nlink == refs + extra

  // File sizes are non-negative.
  invariant sizeNonNeg =
    forall ino in inodes.keys(): inodes.get(ino).size >= 0

  // For regular files, the sum of extent lengths equals the inode size.
  invariant extentSizeConsistency =
    forall ino in inodes.keys():
      inodes.get(ino).fileType == Regular implies
        extentTotal(extentMap.get(ino)) == inodes.get(ino).size

  // Extents within a file do not overlap.
  invariant extentsNonOverlapping =
    forall ino in extentMap.keys():
      extentsDisjoint(extentMap.get(ino))

  // Extents cover exactly [0, size) with no gaps.
  // (Equivalently: every byte offset in [0, size) is covered by exactly one extent.)
  invariant extentsCoverFile =
    forall ino in inodes.keys():
      inodes.get(ino).fileType == Regular implies
        forall off in 0.to(inodes.get(ino).size - 1):
          exists e in extentMap.get(ino).toSet():
            e.fileOffset <= off and off < e.fileOffset + e.length

  // nextIno is strictly greater than every allocated inode number.
  invariant nextInoMonotone =
    forall ino in inodes.keys(): ino < nextIno

  // Symlink targets only exist for Symlink inodes.
  invariant symlinkConsistency =
    forall ino in symlinkTargets.keys():
      ino.in(inodes.keys()) and inodes.get(ino).fileType == Symlink

  // xattrs only exist for known inodes.
  invariant xattrConsistency =
    forall ino in xattrs.keys(): ino.in(inodes.keys())

  // blksize in every inode matches the configured constant.
  invariant blksizeConsistency =
    forall ino in inodes.keys(): inodes.get(ino).blksize == BLOCK_SIZE

  // -------------------------------------------------------------------------
  // Initialisation
  // -------------------------------------------------------------------------

  action init = {
    val rootInode: Inode = {
      ino     : ROOT_INO,
      fileType: Directory,
      mode    : 0o755,
      nlink   : 1,           // counts itself ("." link)
      uid     : 0,
      gid     : 0,
      size    : 0,
      atime   : 0,
      mtime   : 0,
      ctime   : 0,
      blksize : BLOCK_SIZE,
      blocks  : 0,
      rdev    : 0
    }
    val emptySnapshot: IndexSnapshot = {
      inodes        : Map(ROOT_INO -> rootInode),
      dirEntries    : Set(),
      extentMap     : Map(),
      symlinkTargets: Map(),
      xattrs        : Map(),
      nextIno       : ROOT_INO + 1
    }
    inodes'          = emptySnapshot.inodes
    dirEntries'      = emptySnapshot.dirEntries
    extentMap'       = emptySnapshot.extentMap
    symlinkTargets'  = emptySnapshot.symlinkTargets
    xattrs'          = emptySnapshot.xattrs
    nextIno'         = emptySnapshot.nextIno
    diskCache'       = emptySnapshot
    diskCacheDirty'  = false
    remoteCache'     = emptySnapshot
    remoteStoreDirty' = false
  }

  // -------------------------------------------------------------------------
  // Index mutation actions
  // -------------------------------------------------------------------------

  // Allocate the next inode number (pure helper; called inside other actions).
  pure def freshIno(current: int): int = current

  // Create a new inode and add it to the index.
  // Returns (via the state change) an inode with the given attributes
  // assigned the next available ino.
  action createInode(
    fileType: FileType,
    mode    : int,
    uid     : int,
    gid     : int,
    rdev    : int,
    now     : int   // timestamp (nanoseconds since epoch)
  ) = {
    val ino = nextIno
    val newInode: Inode = {
      ino     : ino,
      fileType: fileType,
      mode    : mode,
      nlink   : 0,           // caller must add a DirEntry to make nlink >= 1
      uid     : uid,
      gid     : gid,
      size    : 0,
      atime   : now,
      mtime   : now,
      ctime   : now,
      blksize : BLOCK_SIZE,
      blocks  : 0,
      rdev    : rdev
    }
    inodes'         = inodes.set(ino, newInode)
    extentMap'      = extentMap.set(ino, List())
    nextIno'        = nextIno + 1
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Add a directory entry, incrementing the child's nlink.
  // Precondition: no entry with (parentIno, name) already exists.
  action addDirEntry(parentIno: int, name: str, childIno: int, now: int) = {
    require(parentIno.in(inodes.keys()))
    require(childIno.in(inodes.keys()))
    require(inodes.get(parentIno).fileType == Directory)
    require(not(exists e in dirEntries: e.parentIno == parentIno and e.name == name))
    val newEntry: DirEntry = { parentIno: parentIno, name: name, childIno: childIno }
    val child    = inodes.get(childIno)
    val parent   = inodes.get(parentIno)
    dirEntries'     = dirEntries.union(Set(newEntry))
    inodes'         = inodes
                        .set(childIno, { ...child,  nlink: child.nlink + 1, ctime: now })
                        .set(parentIno,{ ...parent, mtime: now, ctime: now })
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Remove a directory entry, decrementing the child's nlink.
  // If nlink reaches 0 the inode (and its extents) are freed.
  // Precondition: the entry exists; directory entries must be empty first.
  action removeDirEntry(parentIno: int, name: str, now: int) = {
    require(exists e in dirEntries: e.parentIno == parentIno and e.name == name)
    val entry   = dirEntries.filter(e => e.parentIno == parentIno and e.name == name).choose()
    val child   = inodes.get(entry.childIno)
    val parent  = inodes.get(parentIno)
    // Directories must be empty before removal.
    require(
      child.fileType != Directory or
      not(exists e in dirEntries: e.parentIno == entry.childIno)
    )
    val newNlink = child.nlink - 1
    dirEntries' = dirEntries.filter(e => not(e.parentIno == parentIno and e.name == name))
    inodes'     = if (newNlink == 0)
                    // Free the inode: remove from map, extents become orphaned.
                    inodes
                      .remove(entry.childIno)
                      .set(parentIno, { ...parent, mtime: now, ctime: now })
                  else
                    inodes
                      .set(entry.childIno, { ...child, nlink: newNlink, ctime: now })
                      .set(parentIno, { ...parent, mtime: now, ctime: now })
    extentMap'      = if (newNlink == 0)
                        extentMap.remove(entry.childIno)
                      else extentMap
    symlinkTargets' = if (newNlink == 0 and child.fileType == Symlink)
                        symlinkTargets.remove(entry.childIno)
                      else symlinkTargets
    xattrs'         = if (newNlink == 0) xattrs.remove(entry.childIno) else xattrs
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Atomically move a directory entry (POSIX rename semantics).
  // If dstName already exists in dstParent it is atomically replaced.
  action renameDirEntry(
    srcParent: int, srcName: str,
    dstParent: int, dstName: str,
    now      : int
  ) = {
    require(exists e in dirEntries: e.parentIno == srcParent and e.name == srcName)
    val srcEntry = dirEntries
                     .filter(e => e.parentIno == srcParent and e.name == srcName)
                     .choose()
    // Remove any existing destination entry first.
    val withoutDst = dirEntries.filter(e =>
      not(e.parentIno == dstParent and e.name == dstName))
    val withoutSrc = withoutDst.filter(e =>
      not(e.parentIno == srcParent and e.name == srcName))
    val newEntry: DirEntry = { parentIno: dstParent, name: dstName, childIno: srcEntry.childIno }
    dirEntries'     = withoutSrc.union(Set(newEntry))
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Update inode metadata (chmod, chown, utimens, truncate size field, etc.).
  action setAttr(ino: int, updated: Inode, now: int) = {
    require(ino.in(inodes.keys()))
    require(updated.ino == ino)
    inodes'         = inodes.set(ino, { ...updated, ctime: now })
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Append an extent to a regular file's extent list and update size/blocks.
  action appendExtent(ino: int, extent: Extent, now: int) = {
    require(ino.in(inodes.keys()))
    require(inodes.get(ino).fileType == Regular)
    require(extent.length > 0)
    val inode       = inodes.get(ino)
    val newSize     = inode.size + extent.length
    val newBlocks   = (newSize + 511) / 512
    val newExtents  = extentMap.get(ino).append(extent)
    extentMap'      = extentMap.set(ino, newExtents)
    inodes'         = inodes.set(ino, {
                        ...inode,
                        size  : newSize,
                        blocks: newBlocks,
                        mtime : now,
                        ctime : now
                      })
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Truncate a file to newSize bytes by dropping extents beyond that point
  // and adjusting the tail extent if the cut falls within one.
  // Orphaned extent data becomes candidates for compaction.
  action truncateFile(ino: int, newSize: int, now: int) = {
    require(ino.in(inodes.keys()))
    require(inodes.get(ino).fileType == Regular)
    require(newSize >= 0)
    val inode   = inodes.get(ino)
    val trimmed = extentMap.get(ino).filter(e =>
      e.fileOffset < newSize
    ).map(e =>
      if (e.fileOffset + e.length <= newSize)
        e  // extent fully within new size, keep as-is
      else
        // Partial extent: trim length to fit within newSize.
        { ...e, length: newSize - e.fileOffset }
    )
    extentMap' = extentMap.set(ino, trimmed)
    inodes'    = inodes.set(ino, {
                   ...inode,
                   size  : newSize,
                   blocks: (newSize + 511) / 512,
                   mtime : now,
                   ctime : now
                 })
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Record the symlink target for a symlink inode.
  action setSymlinkTarget(ino: int, target: str, now: int) = {
    require(ino.in(inodes.keys()))
    require(inodes.get(ino).fileType == Symlink)
    val inode       = inodes.get(ino)
    symlinkTargets' = symlinkTargets.set(ino, target)
    // Store target length as the inode size (POSIX convention).
    inodes'         = inodes.set(ino, { ...inode, size: target.length(), ctime: now })
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Set or replace a single extended attribute on an inode.
  action setXattr(ino: int, attrName: str, attrValue: str, now: int) = {
    require(ino.in(inodes.keys()))
    val existing  = if (ino.in(xattrs.keys())) xattrs.get(ino) else Map()
    xattrs'         = xattrs.set(ino, existing.set(attrName, attrValue))
    val inode       = inodes.get(ino)
    inodes'         = inodes.set(ino, { ...inode, ctime: now })
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Remove a single extended attribute from an inode.
  action removeXattr(ino: int, attrName: str, now: int) = {
    require(ino.in(inodes.keys()))
    require(ino.in(xattrs.keys()))
    require(attrName.in(xattrs.get(ino).keys()))
    xattrs'         = xattrs.set(ino, xattrs.get(ino).remove(attrName))
    val inode       = inodes.get(ino)
    inodes'         = inodes.set(ino, { ...inode, ctime: now })
    diskCacheDirty'   = true
    remoteStoreDirty' = true
  }

  // Update the access time of an inode (atime update on read).
  action touchAtime(ino: int, now: int) = {
    require(ino.in(inodes.keys()))
    val inode = inodes.get(ino)
    inodes'   = inodes.set(ino, { ...inode, atime: now })
    diskCacheDirty'   = true
    remoteStoreDirty' = remoteStoreDirty  // atime-only: remote store does not need updating
  }

  // -------------------------------------------------------------------------
  // Persistence actions
  // -------------------------------------------------------------------------

  // Flush the current in-memory index to the on-disk cache.
  // In the implementation this serialises the index to CACHE_DIR.
  action persistToDisk = {
    diskCache' = {
      inodes        : inodes,
      dirEntries    : dirEntries,
      extentMap     : extentMap,
      symlinkTargets: symlinkTargets,
      xattrs        : xattrs,
      nextIno       : nextIno
    }
    diskCacheDirty'   = false
    remoteStoreDirty' = remoteStoreDirty
  }

  // Restore the index from the on-disk cache (used at startup).
  // Precondition: the cache holds a valid snapshot.
  action restoreFromDisk = {
    inodes'           = diskCache.inodes
    dirEntries'       = diskCache.dirEntries
    extentMap'        = diskCache.extentMap
    symlinkTargets'   = diskCache.symlinkTargets
    xattrs'           = diskCache.xattrs
    nextIno'          = diskCache.nextIno
    diskCacheDirty'   = false
    remoteStoreDirty' = remoteStoreDirty
  }

  // Write the current index to the remote object store.
  //
  // Precondition: remoteStoreDirty — this action is only useful (and only
  // enabled) when structural mutations have occurred since the last remote
  // write. Atime-only updates do not set remoteStoreDirty, so read-only
  // workloads never trigger this action and never generate presigned-URL
  // round-trips.
  action persistToRemoteStore = {
    require(remoteStoreDirty)
    remoteCache' = {
      inodes        : inodes,
      dirEntries    : dirEntries,
      extentMap     : extentMap,
      symlinkTargets: symlinkTargets,
      xattrs        : xattrs,
      nextIno       : nextIno
    }
    remoteStoreDirty' = false
  }

  // -------------------------------------------------------------------------
  // Persistence invariants
  // -------------------------------------------------------------------------

  // Once the disk cache is clean, it exactly reflects memory.
  invariant cacheConsistencyWhenClean =
    not(diskCacheDirty) implies (
      diskCache.inodes         == inodes         and
      diskCache.dirEntries     == dirEntries      and
      diskCache.extentMap      == extentMap       and
      diskCache.symlinkTargets == symlinkTargets  and
      diskCache.xattrs         == xattrs          and
      diskCache.nextIno        == nextIno
    )

  // When the remote store is clean, all structural fields match memory.
  // Atime is excluded: TouchAtime does not set remoteStoreDirty, so the
  // remote may hold stale atimes while all structural fields are in sync.
  invariant remoteStoreConsistencyWhenClean =
    not(remoteStoreDirty) implies (
      inodesStructurallyEqual(remoteCache.inodes, inodes) and
      remoteCache.dirEntries     == dirEntries      and
      remoteCache.extentMap      == extentMap       and
      remoteCache.symlinkTargets == symlinkTargets  and
      remoteCache.xattrs         == xattrs          and
      remoteCache.nextIno        == nextIno
    )

  // The disk cache nextIno never exceeds the in-memory nextIno
  // (we never roll back inode allocation).
  invariant nextInoNeverRollsBack =
    diskCache.nextIno <= nextIno
}
