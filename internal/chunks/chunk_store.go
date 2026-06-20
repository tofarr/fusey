package chunks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tofarr/fusey/internal/index"
)

// ChunkStore manages the active chunk buffer and delegates sealed chunk I/O
// to the backing Store. It corresponds to the chunks.qnt state machine.
//
// Invariants maintained (from chunks.qnt):
//   - Exactly one chunk is Active at any time.
//   - The active chunk never exceeds maxChunkSize bytes.
//   - Sealed chunks are immutable.
//
// An optional local disk cache (set via SetCacheDir) stores sealed chunks on
// the pod's local filesystem. Cache reads bypass the backing store entirely;
// on a cache miss the full chunk is fetched once and written to disk so all
// subsequent reads are served locally.
type ChunkStore struct {
	mu           sync.Mutex
	store        Store
	maxChunkSize int64
	cache        *chunkCache // nil when local caching is disabled

	activeID  string // ID of the current active chunk
	activeBuf []byte // in-memory buffer for the active chunk
	nextSeq   int    // monotone counter used to generate chunk IDs
}

// NewChunkStore creates a ChunkStore using store as the backing object store.
// Local chunk caching is disabled by default; call SetCacheDir to enable it.
func NewChunkStore(store Store, maxChunkSize int64) *ChunkStore {
	cs := &ChunkStore{
		store:        store,
		maxChunkSize: maxChunkSize,
		nextSeq:      0,
	}
	cs.activeID = cs.chunkID(cs.nextSeq)
	cs.nextSeq++
	return cs
}

// RecoverNextSeq scans the backing store for existing chunk IDs and advances
// nextSeq past the highest one found. This prevents a new session from
// opening an active chunk whose ID aliases a sealed chunk written by a
// previous session, which would cause reads of that chunk to hit the new
// (empty) active buffer instead of the store or the local cache.
//
// Call once after NewChunkStore before the first Append or Read. On a
// genuinely fresh filesystem with no existing chunks this is a no-op.
// Non-chunk object IDs (compacted-*, index.json) are ignored.
func (cs *ChunkStore) RecoverNextSeq(ctx context.Context) error {
	ids, err := cs.store.List(ctx)
	if err != nil {
		return fmt.Errorf("recover next seq: %w", err)
	}

	maxSeq := -1
	for _, id := range ids {
		var seq int
		if n, _ := fmt.Sscanf(id, "chunk-%08d", &seq); n == 1 && seq > maxSeq {
			maxSeq = seq
		}
	}

	if maxSeq < 0 {
		return nil // no existing chunks; keep the default initialisation
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.nextSeq = maxSeq + 1
	cs.activeID = cs.chunkID(cs.nextSeq)
	cs.nextSeq++
	return nil
}

// SetCacheDir enables local disk caching of sealed chunks under baseDir/chunks/.
// The directory is created if it does not exist. Once enabled, sealed chunk
// reads are served from disk on a cache hit; on a miss the full chunk is
// fetched from the backing store, written to disk, and the result returned.
// When a chunk is sealed locally it is written to the cache immediately so
// the very next read requires no store round-trip.
func (cs *ChunkStore) SetCacheDir(baseDir string) error {
	cache, err := newChunkCache(baseDir)
	if err != nil {
		return err
	}
	cs.mu.Lock()
	cs.cache = cache
	cs.mu.Unlock()
	return nil
}

// chunkID formats a chunk ID from a sequence number.
func (cs *ChunkStore) chunkID(seq int) string {
	return fmt.Sprintf("chunk-%08d", seq)
}

// ActiveID returns the ID of the currently active chunk.
func (cs *ChunkStore) ActiveID() string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.activeID
}

// Append writes data to the active chunk, rotating to a fresh one if necessary.
// It returns an Extent describing where the data was stored within the chunk.
// fileOffset is the logical file offset this data belongs to.
func (cs *ChunkStore) Append(ctx context.Context, data []byte, fileOffset int64) (index.Extent, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if int64(len(data)) > cs.maxChunkSize {
		return index.Extent{}, fmt.Errorf("write of %d bytes exceeds max chunk size %d", len(data), cs.maxChunkSize)
	}

	// Rotate if this write would overflow the active chunk.
	if int64(len(cs.activeBuf))+int64(len(data)) > cs.maxChunkSize {
		if err := cs.sealLocked(ctx); err != nil {
			return index.Extent{}, err
		}
	}

	chunkOffset := int64(len(cs.activeBuf))
	cs.activeBuf = append(cs.activeBuf, data...)

	return index.Extent{
		ChunkID:     cs.activeID,
		ChunkOffset: chunkOffset,
		Length:      int64(len(data)),
		FileOffset:  fileOffset,
	}, nil
}

// Read fetches length bytes at chunkOffset from the chunk with id.
// The active chunk is served from the in-memory buffer without a store round-trip.
func (cs *ChunkStore) Read(ctx context.Context, chunkID string, chunkOffset, length int64) ([]byte, error) {
	cs.mu.Lock()
	active := cs.activeID
	var activeBuf []byte
	if chunkID == active {
		activeBuf = make([]byte, len(cs.activeBuf))
		copy(activeBuf, cs.activeBuf)
	}
	cs.mu.Unlock()

	if chunkID == active {
		if chunkOffset+length > int64(len(activeBuf)) {
			return nil, fmt.Errorf("read out of bounds: chunk %s offset %d length %d size %d",
				chunkID, chunkOffset, length, len(activeBuf))
		}
		return activeBuf[chunkOffset : chunkOffset+length], nil
	}
	// Sealed chunk: serve from local cache if enabled, otherwise fetch directly.
	if cs.cache != nil {
		return cs.cache.readRange(ctx, cs.store, chunkID, chunkOffset, length)
	}
	return cs.store.GetRange(ctx, chunkID, chunkOffset, length)
}

// Seal flushes the active chunk to the backing store and opens a fresh one.
// Called explicitly at unmount or by Append on overflow.
func (cs *ChunkStore) Seal(ctx context.Context) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.sealLocked(ctx)
}

// sealLocked flushes the active chunk and starts a new one (called with mu held).
func (cs *ChunkStore) sealLocked(ctx context.Context) error {
	if len(cs.activeBuf) == 0 {
		// Nothing to seal: just rotate the ID so the new active is distinct.
		cs.activeID = cs.chunkID(cs.nextSeq)
		cs.nextSeq++
		return nil
	}
	// Delete before put: FlushActive may have already written a partial
	// version of this chunk; overwrite it with the full buffer.
	sealID := cs.activeID
	sealData := cs.activeBuf
	_ = cs.store.Delete(ctx, sealID)
	if err := cs.store.Put(ctx, sealID, sealData); err != nil {
		return fmt.Errorf("seal chunk %s: %w", sealID, err)
	}
	// Write to local cache so the next read of this chunk is served from disk.
	if cs.cache != nil {
		_ = cs.cache.put(sealID, sealData) // best-effort: miss on failure falls back to store
	}
	cs.activeID = cs.chunkID(cs.nextSeq)
	cs.nextSeq++
	cs.activeBuf = cs.activeBuf[:0]
	return nil
}

// FlushActive ensures any unflushed active-chunk data is written to the store.
// Safe to call multiple times; a no-op if the active buffer is empty.
func (cs *ChunkStore) FlushActive(ctx context.Context) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if len(cs.activeBuf) == 0 {
		return nil
	}
	// Write active buffer to store but keep it as the active chunk (don't rotate).
	// This is used at persist time so reads via the store path work too.
	// We overwrite if it already exists (the store may have a previous partial flush).
	_ = cs.store.Delete(ctx, cs.activeID) // ignore not-found
	return cs.store.Put(ctx, cs.activeID, cs.activeBuf)
}

// DeleteChunks removes the given chunk IDs from the backing store.
// Used by the compactor after remapping live extents to new chunks.
func (cs *ChunkStore) DeleteChunks(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if err := cs.store.Delete(ctx, id); err != nil {
			return fmt.Errorf("delete chunk %s: %w", id, err)
		}
	}
	return nil
}

// PutSealed writes pre-assembled data as a new sealed chunk.
// Used by the compactor to write compacted output.
func (cs *ChunkStore) PutSealed(ctx context.Context, id string, data []byte) error {
	return cs.store.Put(ctx, id, data)
}

// SealedSize returns the size of a sealed chunk from the backing store.
func (cs *ChunkStore) SealedSize(ctx context.Context, id string) (int64, error) {
	cs.mu.Lock()
	active := cs.activeID
	cs.mu.Unlock()
	if id == active {
		cs.mu.Lock()
		n := int64(len(cs.activeBuf))
		cs.mu.Unlock()
		return n, nil
	}
	return cs.store.Size(ctx, id)
}

// ListSealed returns all chunk IDs in the backing store, excluding the active chunk.
func (cs *ChunkStore) ListSealed(ctx context.Context) ([]string, error) {
	cs.mu.Lock()
	activeID := cs.activeID
	cs.mu.Unlock()

	all, err := cs.store.List(ctx)
	if err != nil {
		return nil, err
	}
	sealed := all[:0]
	for _, id := range all {
		if id != activeID {
			sealed = append(sealed, id)
		}
	}
	return sealed, nil
}

// now returns the current time in nanoseconds since epoch.
func now() int64 { return time.Now().UnixNano() }
