// Package compaction implements the background compaction process described in
// compaction.qnt. It identifies sealed chunks with a high orphaned-byte fraction,
// copies live extents into a fresh compacted chunk, remaps the index, and
// deletes the original chunks.
//
// Safety invariant (allLiveExtentsPreserved): the index is persisted durably
// before any old chunk is deleted, so a crash between the two steps leaves the
// filesystem in a consistent state (old chunks are still present and the index
// already points at the new ones — duplicated data that is cleaned up on the
// next compaction run).
package compaction

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tofarr/fusey/internal/chunks"
	"github.com/tofarr/fusey/internal/index"
)

// Persist is a function that durably persists the index to disk.
// Supplied by the caller so the compactor doesn't depend on the cache path.
type Persist func() error

// Compactor selects orphan-heavy sealed chunks, copies their live extents into
// a new chunk, updates the index, and deletes the originals.
type Compactor struct {
	idx       *index.Index
	cs        *chunks.ChunkStore
	persist   Persist
	threshold float64       // orphan fraction above which a chunk is targeted
	interval  time.Duration // how often to run
}

// New creates a Compactor.
func New(idx *index.Index, cs *chunks.ChunkStore, persist Persist, threshold float64, interval time.Duration) *Compactor {
	return &Compactor{
		idx:       idx,
		cs:        cs,
		persist:   persist,
		threshold: threshold,
		interval:  interval,
	}
}

// Run starts the compaction loop, running until ctx is cancelled.
func (c *Compactor) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Compact(ctx); err != nil {
				log.Printf("compaction error: %v", err)
			}
		}
	}
}

// Compact runs one compaction cycle. It is safe to call directly (e.g. in tests).
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

	// 3. Build remap: for each live extent in a target chunk, assign it a
	//    position in the compacted output.
	type key struct {
		chunkID     string
		chunkOffset int64
	}
	remap := make(map[[2]interface{}]index.Extent)

	// We write all compacted data into a new sealed chunk via PutSealed.
	compactedID := fmt.Sprintf("compacted-%d", time.Now().UnixNano())
	var compactedBuf []byte

	targetSet := make(map[string]bool, len(targets))
	for _, id := range targets {
		targetSet[id] = true
	}

	for _, id := range targets {
		for _, e := range liveByChunk[id] {
			data, err := c.cs.Read(ctx, e.ChunkID, e.ChunkOffset, e.Length)
			if err != nil {
				return fmt.Errorf("read extent from %s: %w", id, err)
			}
			newChunkOffset := int64(len(compactedBuf))
			compactedBuf = append(compactedBuf, data...)
			remap[[2]interface{}{e.ChunkID, e.ChunkOffset}] = index.Extent{
				ChunkID:     compactedID,
				ChunkOffset: newChunkOffset,
				Length:      e.Length,
				FileOffset:  e.FileOffset,
			}
		}
	}

	// 4. Write the compacted chunk to the store (if it contains any live data).
	if len(compactedBuf) > 0 {
		if err := c.cs.PutSealed(ctx, compactedID, compactedBuf); err != nil {
			return fmt.Errorf("write compacted chunk: %w", err)
		}
	}

	// 5. Remap the index (atomically updates all affected extent references).
	c.idx.RemapExtents(remap)

	// 6. CRITICAL: persist the index BEFORE deleting old chunks.
	//    If we crash here, old chunks still exist and the index already points
	//    at the new locations (duplicate data cleaned up on next compaction).
	if err := c.persist(); err != nil {
		return fmt.Errorf("persist index before chunk deletion: %w", err)
	}

	// 7. Delete the original target chunks.
	if err := c.cs.DeleteChunks(ctx, targets); err != nil {
		return fmt.Errorf("delete compacted chunks: %w", err)
	}

	return nil
}
