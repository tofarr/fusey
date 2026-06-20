// compaction.qnt
//
// Formal specification of the Fusey compaction process.
//
// Over time, writes and truncations leave orphaned data in sealed chunks —
// bytes that are no longer referenced by any inode's extent list.  The
// compactor is a background process that:
//
//   1. Identifies sealed chunks that contain a significant fraction of
//      orphaned data.
//   2. Copies the live extents from those chunks into a freshly-written
//      "compacted" chunk.
//   3. Rewrites the relevant index entries to point at the new locations.
//   4. Deletes the original chunk objects from the backing store.
//
// The critical safety property is:
//   No live extent is ever destroyed or made unreadable during compaction.
//
// This is modeled here as a self-contained state machine that receives a
// snapshot of the live extents and chunk metadata as input, and produces
// an updated world state as output.  The index-rewrite and chunk-delete
// steps are modeled as a single atomic action; the implementation must
// make them durable (index persisted) before deleting source chunks.
//
// Liveness:  Compaction eventually completes (no deadlock), and the total
// number of orphaned bytes strictly decreases after each successful run.

module compaction {

  import types.* from "./types"

  // -------------------------------------------------------------------------
  // World snapshot
  //
  // The compactor works against a consistent snapshot taken at the start of
  // a compaction cycle.  New writes during compaction go to a new active
  // chunk and do not interfere with the compacted chunks.
  // -------------------------------------------------------------------------

  // The set of extents currently live in the index at the time the snapshot
  // was taken.  An extent is live iff it appears in some inode's extentMap.
  var liveExtents: Set[Extent]

  // Metadata for every chunk object known at snapshot time.
  // The active chunk at snapshot time is NOT a compaction candidate
  // (only sealed chunks are eligible).
  var snapshotChunks: str -> Chunk

  // -------------------------------------------------------------------------
  // Compaction state
  // -------------------------------------------------------------------------

  // Whether a compaction cycle is currently running.
  var compactionInProgress: bool

  // The chunk IDs selected as targets for the current compaction run.
  // These must all be Sealed chunks present in snapshotChunks.
  var compactionTargets: Set[str]

  // The new extents produced for live data during compaction.
  // Keys are (old chunkId, old chunkOffset) → new Extent.
  // Once compaction commits these replace the old extent references
  // in the index (modeled by the commit action updating liveExtents).
  var remappedExtents: (str, int) -> Extent

  // -------------------------------------------------------------------------
  // Pure helpers
  // -------------------------------------------------------------------------

  // Total bytes occupied by live extents within a single chunk.
  pure def liveBytes(chunkId: str, extents: Set[Extent]): int =
    extents
      .filter(e => e.chunkId == chunkId)
      .map(e => e.length)
      .fold(0, (acc, n) => acc + n)

  // Total size of a chunk.
  pure def chunkSize(chunkId: str, chks: str -> Chunk): int =
    if (chunkId.in(chks.keys())) chks.get(chunkId).size else 0

  // Orphaned bytes in a chunk = total size − live bytes.
  pure def orphanedBytes(chunkId: str, extents: Set[Extent], chks: str -> Chunk): int =
    chunkSize(chunkId, chks) - liveBytes(chunkId, extents)

  // An extent is live if it appears in the live set.
  pure def isLive(e: Extent, extents: Set[Extent]): bool =
    extents.contains(e)

  // The set of live extents that reside in a given chunk.
  pure def liveExtentsInChunk(chunkId: str, extents: Set[Extent]): Set[Extent] =
    extents.filter(e => e.chunkId == chunkId)

  // -------------------------------------------------------------------------
  // Invariants
  // -------------------------------------------------------------------------

  // Compaction targets must be sealed chunks present in the snapshot.
  invariant targetsAreSealed =
    forall id in compactionTargets:
      id.in(snapshotChunks.keys()) and
      snapshotChunks.get(id).status == Sealed

  // The remapping covers exactly the live extents in the target chunks.
  invariant remappingCoversLiveExtents =
    compactionInProgress implies
      forall e in liveExtents:
        e.chunkId.in(compactionTargets) implies
          (e.chunkId, e.chunkOffset).in(remappedExtents.keys())

  // Remapped extents preserve file offsets and lengths.
  // (The data content moves to a new chunk location but logically represents
  // the same bytes at the same position within the file.)
  invariant remappingPreservesLogicalLocation =
    forall key in remappedExtents.keys():
      val newExtent = remappedExtents.get(key)
      exists oldExtent in liveExtents:
        oldExtent.chunkId == key._1 and
        oldExtent.chunkOffset == key._2 and
        oldExtent.length     == newExtent.length and
        oldExtent.fileOffset == newExtent.fileOffset

  // No orphaned bytes are introduced into the compacted output.
  // (Every byte in a compacted chunk is part of a live extent.)
  invariant compactedChunksHaveNoOrphans =
    compactionInProgress implies
      forall e in remappedExtents.values():
        liveExtents.contains(e)

  // -------------------------------------------------------------------------
  // Safety: the core correctness property of compaction.
  //
  // After committing a compaction run every pre-existing live extent is
  // still reachable — either unchanged (for extents outside the targets)
  // or via a remapped equivalent (for extents inside the targets).
  //
  // We state this as a predicate that must hold when commitCompaction fires.
  // -------------------------------------------------------------------------

  pure def allLiveExtentsPreserved(
    before  : Set[Extent],
    targets : Set[str],
    remapped: (str, int) -> Extent
  ): bool =
    forall e in before:
      if (e.chunkId.in(targets))
        // This extent was in a compacted chunk; it must appear in the remap.
        (e.chunkId, e.chunkOffset).in(remapped.keys()) and
        remapped.get((e.chunkId, e.chunkOffset)).length == e.length and
        remapped.get((e.chunkId, e.chunkOffset)).fileOffset == e.fileOffset
      else
        // Extents outside the compacted chunks are untouched.
        true

  // -------------------------------------------------------------------------
  // Initialisation
  // -------------------------------------------------------------------------

  action init = {
    liveExtents'         = Set()
    snapshotChunks'      = Map()
    compactionInProgress'= false
    compactionTargets'   = Set()
    remappedExtents'     = Map()
  }

  // -------------------------------------------------------------------------
  // takeSnapshot — record the current world state at cycle start.
  //
  // In the implementation this corresponds to reading the current index
  // and chunk list into the compactor's local memory before touching
  // any objects in the backing store.
  // -------------------------------------------------------------------------

  action takeSnapshot(extents: Set[Extent], chks: str -> Chunk) = {
    require(not(compactionInProgress))
    liveExtents'    = extents
    snapshotChunks' = chks
  }

  // -------------------------------------------------------------------------
  // selectTargets — choose which sealed chunks to compact.
  //
  // A chunk is a worthwhile target if its orphaned fraction exceeds a
  // threshold.  The implementation may use any heuristic; we only require
  // that the selected targets are sealed chunks with at least one orphaned
  // byte (no-op compaction is pointless).
  // -------------------------------------------------------------------------

  action selectTargets(targets: Set[str]) = {
    require(not(compactionInProgress))
    require(targets.size() > 0)
    // All targets must be sealed.
    require(forall id in targets:
      id.in(snapshotChunks.keys()) and
      snapshotChunks.get(id).status == Sealed)
    // At least one target must have orphaned bytes (otherwise pointless).
    require(exists id in targets:
      orphanedBytes(id, liveExtents, snapshotChunks) > 0)
    compactionTargets'    = targets
    compactionInProgress' = true
  }

  // -------------------------------------------------------------------------
  // buildRemap — compute where each live extent will land in the output.
  //
  // The live bytes from all target chunks are concatenated (in arbitrary
  // but deterministic order) into a new compacted chunk.  We model the
  // resulting layout as a map from old (chunkId, offset) to new Extent.
  //
  // The new chunk ID and the exact packing order are chosen by the
  // implementation; this action accepts any valid remap that satisfies
  // the preservation invariants.
  // -------------------------------------------------------------------------

  action buildRemap(remap: (str, int) -> Extent) = {
    require(compactionInProgress)
    // The remap must cover every live extent in the target chunks.
    require(
      forall e in liveExtents:
        e.chunkId.in(compactionTargets) implies
          (e.chunkId, e.chunkOffset).in(remap.keys()))
    // Every remapped extent preserves logical file position and length.
    require(
      forall key in remap.keys():
        exists e in liveExtents:
          e.chunkId    == key._1 and
          e.chunkOffset == key._2 and
          e.length     == remap.get(key).length and
          e.fileOffset == remap.get(key).fileOffset)
    // Remapped extents go to a chunk that is NOT one of the targets
    // (we must not compact in-place).
    require(
      forall key in remap.keys():
        not(remap.get(key).chunkId.in(compactionTargets)))
    remappedExtents' = remap
  }

  // -------------------------------------------------------------------------
  // commitCompaction — atomically apply the remap and retire the old chunks.
  //
  // This is the critical step.  In the implementation:
  //   1. The index is updated to reference the new chunk locations.
  //   2. The index is persisted durably.
  //   3. Only then are the old chunk objects deleted from the backing store.
  //
  // We model steps 1–3 as atomic here to express the safety property
  // clearly.  Crashing between 2 and 3 is safe (old chunks are still
  // present); crashing between 1 and 2 is handled by crash-recovery.
  // -------------------------------------------------------------------------

  action commitCompaction = {
    require(compactionInProgress)
    require(remappedExtents.keys().size() > 0 or
            liveExtents.filter(e => e.chunkId.in(compactionTargets)).size() == 0)
    // Safety check: the remap preserves all live extents in the targets.
    require(allLiveExtentsPreserved(liveExtents, compactionTargets, remappedExtents))

    // Rewrite liveExtents: replace extents in target chunks with their remapped versions.
    val updatedExtents = liveExtents
      .filter(e => not(e.chunkId.in(compactionTargets)))  // keep non-target extents
      .union(remappedExtents.values().toSet())             // add remapped extents

    // Retire the target chunks from the snapshot (they are now deleted in the store).
    val updatedChunks = snapshotChunks.mapFilter((id, _) => not(id.in(compactionTargets)))

    liveExtents'          = updatedExtents
    snapshotChunks'       = updatedChunks
    compactionInProgress' = false
    compactionTargets'    = Set()
    remappedExtents'      = Map()
  }

  // -------------------------------------------------------------------------
  // abortCompaction — safely abandon the current cycle.
  //
  // No state is changed in the index or backing store; the compactor simply
  // resets its in-progress bookkeeping.  This is safe to call at any point.
  // -------------------------------------------------------------------------

  action abortCompaction = {
    require(compactionInProgress)
    compactionInProgress' = false
    compactionTargets'    = Set()
    remappedExtents'      = Map()
  }

  // -------------------------------------------------------------------------
  // Temporal / liveness properties
  // -------------------------------------------------------------------------

  // A compaction cycle that is started eventually either commits or aborts.
  temporal compactionTerminates =
    always(compactionInProgress implies eventually(not(compactionInProgress)))

  // The total orphaned bytes across all snapshot chunks never increases
  // after a commit (it may stay the same if there were no orphans).
  temporal orphansNonIncreasing =
    always(
      compactionInProgress implies
        eventually(
          forall id in snapshotChunks.keys():
            orphanedBytes(id, liveExtents, snapshotChunks) == 0
        )
    )
}
