package journal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	cbor "github.com/fxamacker/cbor/v2"

	"github.com/tofarr/fusey/internal/chunks"
	"github.com/tofarr/fusey/internal/index"
)

// Shard is a sequence of journal entries on disk (or in the object store),
// named by its starting seq number. Shards are append-only: a flush always
// creates a new shard, never overwrites an existing one. This is the same
// pattern the ChunkStore uses for chunk objects.
//
// On disk: {prefix}journal-NNNNNNNNN.cbor
// In the local cache: <cacheDir>/journal/journal-NNNNNNNNN.cbor
//
// The shard name and parser are defined in the chunks package
// (chunks.JournalShardName, chunks.ParseJournalShardName) so that the
// broker and S3 store can use them without depending on the journal
// package (which would create an import cycle).

// parseShardName is a thin wrapper that defers to the chunks package.
func parseShardName(name string) (uint64, bool) {
	return chunks.ParseJournalShardName(name)
}

// Flush serialises the journal's current in-memory buffer as a single new
// shard in the object store (and, optionally, to a local cache directory).
// The shard filename encodes the starting seq, so shards are discovered and
// ordered by listing the prefix.
//
// On error, the in-memory buffer is left intact so the next flush attempt
// can retry.
func Flush(ctx context.Context, j *Journal, store chunks.ObjectStore) (shardSeq uint64, entryCount int, err error) {
	batch := j.drainBuffer()
	if len(batch) == 0 {
		return 0, 0, nil
	}
	data, err := cbor.Marshal(batch)
	if err != nil {
		// Put the entries back so they can be retried.
		j.mu.Lock()
		j.buffer = append(batch, j.buffer...)
		j.mu.Unlock()
		return 0, 0, fmt.Errorf("marshal journal shard: %w", err)
	}
	key := store.JournalKey(batch[0].Seq)
	if err := store.PutRaw(ctx, key, data); err != nil {
		// Put the entries back.
		j.mu.Lock()
		j.buffer = append(batch, j.buffer...)
		j.mu.Unlock()
		return 0, 0, fmt.Errorf("write journal shard %s: %w", key, err)
	}
	return batch[0].Seq, len(batch), nil
}

// LoadAll reads every journal shard from the object store, returning the
// entries in seq order. A missing journal (no shards) is not an error: it
// returns a nil slice. Used on startup to rebuild the in-memory journal
// from durable state.
func LoadAll(ctx context.Context, store chunks.ObjectStore) ([]Entry, error) {
	ids, err := store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list journal shards: %w", err)
	}

	prefix := store.Prefix()
	var shardIDs []string
	for _, id := range ids {
		// Strip the prefix (if any) to get the local filename/ID.
		basename := id
		if prefix != "" {
			if !strings.HasPrefix(id, prefix) {
				continue
			}
			basename = id[len(prefix):]
		}
		if _, ok := parseShardName(basename); !ok {
			continue
		}
		shardIDs = append(shardIDs, id)
	}

	if len(shardIDs) == 0 {
		return nil, nil
	}

	// Sort by the starting seq (encoded in the filename) so we apply in order.
	type sortable struct {
		id  string
		seq uint64
	}
	sorted := make([]sortable, len(shardIDs))
	for i, id := range shardIDs {
		basename := id
		if prefix != "" {
			basename = id[len(prefix):]
		}
		seq, _ := parseShardName(basename)
		sorted[i] = sortable{id: id, seq: seq}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].seq < sorted[j].seq })

	var all []Entry
	for _, s := range sorted {
		data, err := store.GetRaw(ctx, s.id)
		if err != nil {
			return nil, fmt.Errorf("read journal shard %s: %w", s.id, err)
		}
		var batch []Entry
		if err := cbor.Unmarshal(data, &batch); err != nil {
			return nil, fmt.Errorf("unmarshal journal shard %s: %w", s.id, err)
		}
		all = append(all, batch...)
	}
	return all, nil
}

// Clear deletes every journal shard from the object store (and, if a local
// cache dir is given, the local journal/ subdirectory). Called at the START
// of `fusey compact`, BEFORE any chunk remap.
//
// The base snapshot is invalidated by Clear (and the caller is expected to
// write a fresh one at the end of compact, if it wants as-of replay to
// remain available).
func Clear(ctx context.Context, store chunks.ObjectStore, cacheDir string) error {
	ids, err := store.List(ctx)
	if err != nil {
		return fmt.Errorf("list objects to clear journal: %w", err)
	}
	prefix := store.Prefix()
	for _, id := range ids {
		basename := id
		if prefix != "" {
			if !strings.HasPrefix(id, prefix) {
				continue
			}
			basename = id[len(prefix):]
		}
		if _, ok := parseShardName(basename); !ok {
			continue
		}
		if err := store.DeleteJournalKey(ctx, id); err != nil {
			return fmt.Errorf("delete journal shard %s: %w", id, err)
		}
	}

	if cacheDir != "" {
		journalDir := filepath.Join(cacheDir, "journal")
		if err := os.RemoveAll(journalDir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("clear local journal cache: %w", err)
		}
	}
	return nil
}

// --- Base snapshot (the post-compact starting point for as-of replay) ---

const baseKeySuffix = "index-base.cbor"

// BaseKey returns the remote object-store key for the base snapshot. The
// caller composes this with the store's prefix; we don't bake the prefix
// in here because the store applies it at PutRaw time.
func BaseKey() string { return baseKeySuffix }

// SaveBase serialises a snapshot of the index for use as the as-of replay
// base. Writes to the local cache and the remote object store.
func SaveBase(idx *index.Index, cacheDir string, store chunks.ObjectStore) error {
	data, err := index.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal base snapshot: %w", err)
	}
	if cacheDir != "" {
		if err := os.MkdirAll(cacheDir, 0o755); err != nil {
			return fmt.Errorf("mkdir base cache: %w", err)
		}
		path := filepath.Join(cacheDir, baseKeySuffix)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return fmt.Errorf("write base cache: %w", err)
		}
	}
	if store != nil {
		if err := store.PutRaw(context.Background(), baseKeySuffix, data); err != nil {
			return fmt.Errorf("write base to store: %w", err)
		}
	}
	return nil
}

// LoadBase reads the base snapshot from the local cache first, then falls
// back to the object store. Returns (nil, os.ErrNotExist) if no base is
// present anywhere. The caller is expected to handle the missing case
// (typically: error out, since as-of mount requires the base).
func LoadBase(ctx context.Context, cacheDir string, store chunks.ObjectStore) (*index.Index, error) {
	if cacheDir != "" {
		path := filepath.Join(cacheDir, baseKeySuffix)
		data, err := os.ReadFile(path)
		if err == nil {
			idx, err := index.Unmarshal(data, 0)
			if err != nil {
				return nil, fmt.Errorf("unmarshal base from local cache: %w", err)
			}
			return idx, nil
		}
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read base from local cache: %w", err)
		}
	}
	if store == nil {
		return nil, os.ErrNotExist
	}
	data, err := store.GetRaw(ctx, baseKeySuffix)
	if err != nil {
		return nil, err
	}
	idx, err := index.Unmarshal(data, 0)
	if err != nil {
		return nil, fmt.Errorf("unmarshal base from store: %w", err)
	}
	if cacheDir != "" {
		_ = os.MkdirAll(cacheDir, 0o755)
		_ = os.WriteFile(filepath.Join(cacheDir, baseKeySuffix), data, 0o644)
	}
	return idx, nil
}
