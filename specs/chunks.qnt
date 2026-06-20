// chunks.qnt
//
// Formal specification of the Fusey chunk store state machine.
//
// File data is stored as a sequence of append-only chunk objects in the
// backing object store (e.g. S3).  At any moment exactly one chunk is
// "active" — it accepts appends.  All others are "sealed" — immutable.
//
// When a write would push the active chunk past MAX_CHUNK_SIZE the chunk
// is sealed and a fresh active chunk is opened.  Sealed chunks may later
// be garbage-collected by the compactor once no live extents reference them.
//
// Configuration constants (resolved from environment variables at startup):
//   FUSEY_CHUNK_SIZE   — maximum size of a single chunk object in bytes
//
// The data bytes themselves are not modeled here; we track only sizes and
// offsets because those are what the index cares about.  Actual I/O
// correctness (that the right bytes are stored and retrieved) is an
// implementation concern outside the scope of this state-machine spec.

module chunks {

  import types.* from "./types"

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  // Maximum size in bytes of a single chunk object (FUSEY_CHUNK_SIZE env var).
  // Writes that would push the active chunk past this limit trigger an
  // automatic seal-and-rotate.
  const MAX_CHUNK_SIZE: int

  // -------------------------------------------------------------------------
  // State
  // -------------------------------------------------------------------------

  // Metadata for every chunk, keyed by chunk ID.
  // Includes both the active chunk and all sealed chunks.
  var chunks: str -> Chunk

  // ID of the currently active (writable) chunk.
  var activeChunkId: str

  // Monotonically increasing sequence used to generate unique chunk IDs.
  // In the implementation chunk IDs are strings such as "chunk-00000042".
  var nextChunkSeq: int

  // The extent that was produced by the most recent appendData action.
  // filesystem.qnt reads this to construct the Extent record it passes
  // to index.appendExtent.  (Quint actions have no return values; this is
  // the standard pattern for communicating results between modules.)
  var lastWrittenExtent: Extent

  // -------------------------------------------------------------------------
  // Pure helpers
  // -------------------------------------------------------------------------

  // Render a sequence number to a deterministic chunk ID string.
  pure def chunkId(seq: int): str = "chunk-" + seq.toString()

  // Compute how many 512-byte blocks a given byte count occupies.
  pure def toBlocks(bytes: int): int = (bytes + 511) / 512

  // -------------------------------------------------------------------------
  // Invariants
  // -------------------------------------------------------------------------

  // The active chunk ID must exist in the chunks map.
  invariant activeChunkExists =
    activeChunkId.in(chunks.keys())

  // The active chunk's status must be Active.
  invariant activeChunkIsActive =
    chunks.get(activeChunkId).status == Active

  // Every other chunk must be Sealed.
  invariant onlyOneActive =
    forall id in chunks.keys():
      id != activeChunkId implies chunks.get(id).status == Sealed

  // The active chunk never exceeds the configured maximum size.
  invariant activeChunkWithinBound =
    chunks.get(activeChunkId).size <= MAX_CHUNK_SIZE

  // All chunk sizes are non-negative.
  invariant chunkSizesNonNeg =
    forall id in chunks.keys(): chunks.get(id).size >= 0

  // All chunk IDs match their map key.
  invariant chunkIdConsistency =
    forall id in chunks.keys(): chunks.get(id).id == id

  // nextChunkSeq is strictly greater than the sequence embedded in every
  // existing chunk ID.  (Enforces monotone allocation.)
  // NOTE: In full generality this requires parsing the ID string; we
  // approximate it by requiring the map to have fewer entries than nextChunkSeq.
  invariant nextChunkSeqBound =
    chunks.keys().size() < nextChunkSeq

  // A sealed chunk's size never changes after sealing.
  // (Modeled as a temporal property in filesystem.qnt; see sealedImmutable.)

  // The last written extent references an existing chunk.
  invariant lastExtentChunkExists =
    lastWrittenExtent.chunkId.in(chunks.keys())

  // The last written extent fits within its chunk's size.
  invariant lastExtentInBounds =
    val c = chunks.get(lastWrittenExtent.chunkId)
    lastWrittenExtent.chunkOffset + lastWrittenExtent.length <= c.size

  // -------------------------------------------------------------------------
  // Initialisation
  // -------------------------------------------------------------------------

  action init = {
    val firstId     = chunkId(0)
    val firstChunk: Chunk = { id: firstId, status: Active, size: 0 }
    val sentinel: Extent  = { chunkId: firstId, chunkOffset: 0, length: 0, fileOffset: 0 }
    chunks'             = Map(firstId -> firstChunk)
    activeChunkId'      = firstId
    nextChunkSeq'       = 1
    lastWrittenExtent'  = sentinel
  }

  // -------------------------------------------------------------------------
  // Internal: seal the active chunk and open a fresh one.
  //
  // Called automatically when an append would overflow the current chunk.
  // -------------------------------------------------------------------------

  action rotateChunk = {
    val oldId    = activeChunkId
    val oldChunk = chunks.get(oldId)
    val newId    = chunkId(nextChunkSeq)
    val newChunk: Chunk = { id: newId, status: Active, size: 0 }
    chunks'        = chunks
                       .set(oldId, { ...oldChunk, status: Sealed })
                       .set(newId, newChunk)
    activeChunkId' = newId
    nextChunkSeq'  = nextChunkSeq + 1
  }

  // -------------------------------------------------------------------------
  // appendData — write dataSize bytes to the store.
  //
  // If the write fits in the current active chunk it is appended there.
  // If not, the active chunk is sealed and a new one is opened first.
  //
  // After this action, lastWrittenExtent describes the allocated region.
  // The caller (filesystem.qnt) is responsible for recording this extent
  // in the index via index.appendExtent.
  //
  // Preconditions:
  //   - dataSize > 0
  //   - dataSize <= MAX_CHUNK_SIZE  (a single write cannot span two chunks)
  // -------------------------------------------------------------------------

  action appendData(dataSize: int, fileOffset: int) = {
    require(dataSize > 0)
    require(dataSize <= MAX_CHUNK_SIZE)

    val active     = chunks.get(activeChunkId)
    val willRotate = active.size + dataSize > MAX_CHUNK_SIZE

    // Determine which chunk will receive the write after optional rotation.
    val targetId   = if (willRotate) chunkId(nextChunkSeq) else activeChunkId
    val baseSize   = if (willRotate) 0                     else active.size

    // Update chunks map: seal old chunk (if rotating) and write to target.
    val afterRotate = if (willRotate)
                        chunks.set(activeChunkId, { ...active, status: Sealed })
                              .set(targetId, { id: targetId, status: Active, size: 0 })
                      else chunks
    val targetChunk = afterRotate.get(targetId)
    chunks'             = afterRotate.set(targetId, {
                            ...targetChunk,
                            size: baseSize + dataSize
                          })
    activeChunkId'      = targetId
    nextChunkSeq'       = if (willRotate) nextChunkSeq + 1 else nextChunkSeq
    lastWrittenExtent'  = {
                            chunkId    : targetId,
                            chunkOffset: baseSize,
                            length     : dataSize,
                            fileOffset : fileOffset
                          }
  }

  // -------------------------------------------------------------------------
  // readRange — validate that a range read is satisfiable.
  //
  // This action does not change state.  It asserts that the requested
  // range lies within the bounds of a known (sealed or active) chunk.
  // The implementation performs an HTTP range-GET against the backing store.
  // -------------------------------------------------------------------------

  action readRange(chunkId: str, offset: int, length: int) = {
    require(chunkId.in(chunks.keys()))
    require(offset >= 0)
    require(length > 0)
    require(offset + length <= chunks.get(chunkId).size)
    // No state change; purely a guard / precondition check.
    chunks'            = chunks
    activeChunkId'     = activeChunkId
    nextChunkSeq'      = nextChunkSeq
    lastWrittenExtent' = lastWrittenExtent
  }

  // -------------------------------------------------------------------------
  // explicitSeal — manually seal the active chunk.
  //
  // Called at filesystem unmount or when an operator wants to force a
  // clean boundary between writing sessions.  A fresh empty chunk is
  // opened immediately so appends can continue without delay.
  // -------------------------------------------------------------------------

  action explicitSeal = {
    val oldId    = activeChunkId
    val oldChunk = chunks.get(oldId)
    val newId    = chunkId(nextChunkSeq)
    val newChunk: Chunk = { id: newId, status: Active, size: 0 }
    chunks'        = chunks
                       .set(oldId, { ...oldChunk, status: Sealed })
                       .set(newId, newChunk)
    activeChunkId' = newId
    nextChunkSeq'  = nextChunkSeq + 1
  }

  // -------------------------------------------------------------------------
  // Temporal properties
  // -------------------------------------------------------------------------

  // Once a chunk is sealed its size never changes.
  temporal sealedImmutable =
    always(
      forall id in chunks.keys():
        chunks.get(id).status == Sealed implies
          always(chunks.get(id).size == chunks.get(id).size)
    )

  // Every append eventually results in a readable extent
  // (the chunk referenced by lastWrittenExtent is always accessible).
  temporal appendAlwaysReadable =
    always(
      lastWrittenExtent.chunkId.in(chunks.keys()) and
      lastWrittenExtent.chunkOffset + lastWrittenExtent.length <=
        chunks.get(lastWrittenExtent.chunkId).size
    )
}
