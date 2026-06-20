// Package compaction implements the compaction process described in
// compaction.qnt. It identifies sealed chunks with a high orphaned-byte
// fraction, copies live extents into fresh compacted chunks (respecting
// chunkSize), remaps the index, and deletes the original chunks.
//
// Compaction is designed to be run on demand — e.g. as a Kubernetes CronJob
// via `fusey compact` — rather than as a background goroutine inside the
// filesystem process. This keeps the filesystem process lightweight and gives
// operators control over when the S3 read/write cost is incurred.
//
// Safety invariant (allLiveExtentsPreserved): the index is persisted durably
// before any old chunk is deleted, so a crash between the two steps leaves the
// filesystem in a consistent state (old chunks are still present and the index
// already points at the new ones — duplicated data cleaned up on the next run).
package compaction

import (
	"context"
	"fmt"
	"time"

	"github.com/tofarr/fusey/internal/chunks"
	"github.com/tofarr/fusey/internal/index"
)

// Persist is a function that durably persists the index.
// Supplied by the caller so the compactor is independent of storage paths.
type Persist func() error

// Compactor selects orphan-heavy sealed chunks, copies their live extents into
// new compacted chunks no larger than chunkSize, remaps the index, and deletes
// the originals.
type Compactor struct {
	idx       *index.Index
	cs        *chunks.ChunkStore
	persist   Persist
	threshold float64 // orphan fraction above which a chunk is targeted
	chunkSize int64   // maximum bytes per compacted output chunk
}

// New creates a Compactor. chunkSize should match FUSEY_CHUNK_SIZE so that
// compacted output chunks are the same size as freshly written chunks.
func New(idx *index.Index, cs *chunks.ChunkStore, persist Persist, threshold float64, chunkSize int64) *Compactor {
	return &Compactor{
		idx:       idx,
		cs:        cs,
		persist:   persist,
		threshold: threshold,
		chunkSize: chunkSize,
	}
}

// Compact runs one compaction cycle. It is safe to call directly.
func (c *Compactor) Compact(ctx context.Context) error {
	// 1. Snapshot live extents from the index.
	liveExts := c.idx.LiveExtents()
	liveByChunk := make(map[string][]index.Extent)
	for _, e := range liveExts {
		liveByChunk[e.ChunkID] = append(liveByChunk[e.ChunkID], e)
	}

	// 2. List sealed chunks and compute their orphan fractions.
	sealedIDs, err := c.cs.ListSealed(ctx)
	if err != nil {
		return fmt.Errorf("list sealed chunks: %w", err)
	}

	var targets []string
	for _, id := range sealedIDs {
		total, err := c.cs.SealedSize(ctx, id)
		if err != nil {
			continue // chunk may have been deleted concurrently
		}
		if total == 0 {
			targets = append(targets, id) // empty sealed chunk: always delete
			continue
		}
		var live int64
		for _, e := range liveByChunk[id] {
			live += e.Length
		}
		orphanFrac := float64(total-live) / float64(total)
		if orphanFrac >= c.threshold {
			targets = append(targets, id)
		}
	}
	if len(targets) == 0 {
		return nil
	}

	// 3. Pack live extents from all target chunks into compacted output chunks,
	//    rotating to a new output chunk whenever the current one would exceed
	//    chunkSize. This preserves the chunk-size invariant that the active
	//    chunk store also enforces on fresh writes.
	remap := make(map[[2]interface{}]index.Extent)

	type outputChunk struct {
		id  string
		buf []byte
	}
	var outputs []outputChunk
	seq := 0
	newOutput := func() {
		id := fmt.Sprintf("compacted-%d-%d", time.Now().UnixNano(), seq)
		seq++
		outputs = append(outputs, outputChunk{id: id})
	}
	newOutput() // prime the first output chunk

	for _, id := range targets {
		for _, e := range liveByChunk[id] {
			data, err := c.cs.Read(ctx, e.ChunkID, e.ChunkOffset, e.Length)
			if err != nil {
				return fmt.Errorf("read extent from %s: %w", id, err)
			}
			cur := &outputs[len(outputs)-1]
			// Rotate when this extent would overflow the current output chunk.
			// Guard len(cur.buf) > 0 so an extent larger than chunkSize still
			// gets its own output rather than spinning forever.
			if len(cur.buf) > 0 && int64(len(cur.buf))+e.Length > c.chunkSize {
				newOutput()
				cur = &outputs[len(outputs)-1]
			}
			newChunkOffset := int64(len(cur.buf))
			cur.buf = append(cur.buf, data...)
			remap[[2]interface{}{e.ChunkID, e.ChunkOffset}] = index.Extent{
				ChunkID:     cur.id,
				ChunkOffset: newChunkOffset,
				Length:      e.Length,
				FileOffset:  e.FileOffset,
			}
		}
	}

	// 4. Write non-empty output chunks to the store.
	for i := range outputs {
		if len(outputs[i].buf) == 0 {
			continue
		}
		if err := c.cs.PutSealed(ctx, outputs[i].id, outputs[i].buf); err != nil {
			return fmt.Errorf("write compacted chunk %s: %w", outputs[i].id, err)
		}
	}

	// 5. Remap the index to point at the new chunk locations.
	c.idx.RemapExtents(remap)

	// 6. CRITICAL: persist the index BEFORE deleting old chunks.
	//    If we crash here, old chunks still exist and the index already points
	//    at the new locations — duplicate data cleaned up on the next run.
	if err := c.persist(); err != nil {
		return fmt.Errorf("persist index before chunk deletion: %w", err)
	}

	// 7. Delete the original target chunks.
	if err := c.cs.DeleteChunks(ctx, targets); err != nil {
		return fmt.Errorf("delete compacted chunks: %w", err)
	}

	return nil
}
