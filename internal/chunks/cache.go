package chunks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// chunkCache is a local disk cache for sealed chunk data.
//
// On a read cache miss the full chunk is fetched from the backing store,
// written atomically to disk, and the requested byte range is returned.
// Subsequent reads of any range within the same chunk are served from
// the local file with no network round-trip.
//
// Concurrent cache misses for the same chunk are deduplicated: only one
// fetch is issued regardless of how many goroutines are waiting. All
// waiters receive the result of that single fetch.
//
// Chunks are stored as {dir}/chunks/{chunkID}. The directory is created
// on first use.
type chunkCache struct {
	dir      string
	mu       sync.Mutex
	inflight map[string]*inflightFetch
}

// inflightFetch tracks an in-progress remote fetch for a single chunk.
type inflightFetch struct {
	ready chan struct{} // closed when the fetch completes
	err   error
}

func newChunkCache(baseDir string) (*chunkCache, error) {
	dir := filepath.Join(baseDir, "chunks")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create chunk cache dir %s: %w", dir, err)
	}
	return &chunkCache{
		dir:      dir,
		inflight: make(map[string]*inflightFetch),
	}, nil
}

// readRange returns length bytes starting at offset from chunkID.
//
// If the full chunk is already on disk the slice is read directly from
// the local file. Otherwise the full chunk is fetched from store, written
// to disk (atomic rename so partial writes are never visible), and the
// requested slice is returned. Concurrent misses for the same chunkID
// are collapsed into a single store fetch.
func (c *chunkCache) readRange(ctx context.Context, store Store, chunkID string, offset, length int64) ([]byte, error) {
	path := filepath.Join(c.dir, chunkID)

	// Fast path: chunk is already cached on disk.
	if data, err := readFileSlice(path, offset, length); err == nil {
		return data, nil
	}

	// Slow path: cache miss.
	// Acquire the lock before touching the inflight map. We re-check the
	// file under the lock to close the TOCTOU race: a concurrent goroutine
	// may have completed a fetch (writing the file and removing the inflight
	// entry) between our fast-path check above and this mutex acquisition.
	c.mu.Lock()
	if data, err := readFileSlice(path, offset, length); err == nil {
		c.mu.Unlock()
		return data, nil
	}
	if inf, ok := c.inflight[chunkID]; ok {
		// Another goroutine is already fetching; wait for it.
		c.mu.Unlock()
		select {
		case <-inf.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if inf.err != nil {
			return nil, inf.err
		}
		return readFileSlice(path, offset, length)
	}
	// We are the designated fetcher for this chunk.
	inf := &inflightFetch{ready: make(chan struct{})}
	c.inflight[chunkID] = inf
	c.mu.Unlock()

	// Fetch the full chunk from the backing store and cache it.
	data, err := fetchFullChunk(ctx, store, chunkID)
	if err == nil {
		err = writeAtomic(path, data)
	}

	// Notify all waiters and remove the in-flight entry.
	inf.err = err
	close(inf.ready)
	c.mu.Lock()
	delete(c.inflight, chunkID)
	c.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return data[offset : offset+length], nil
}

// put writes the full chunk data to the local cache.
// Called when the active chunk is sealed so the newly sealed chunk is
// immediately available for cache reads without a round-trip to the store.
// Errors are non-fatal: a failed cache write means the next read incurs a
// store fetch instead of a cache hit.
func (c *chunkCache) put(chunkID string, data []byte) error {
	return writeAtomic(filepath.Join(c.dir, chunkID), data)
}

// fetchFullChunk retrieves the complete contents of chunkID from store.
// Two round-trips (Size then GetRange) are used to avoid adding a Get method
// to the Store interface. This cost is paid only on a cache miss — once cached
// the chunk is never fetched again for the lifetime of the pod.
func fetchFullChunk(ctx context.Context, store Store, chunkID string) ([]byte, error) {
	size, err := store.Size(ctx, chunkID)
	if err != nil {
		return nil, fmt.Errorf("cache miss size %s: %w", chunkID, err)
	}
	data, err := store.GetRange(ctx, chunkID, 0, size)
	if err != nil {
		return nil, fmt.Errorf("cache miss fetch %s: %w", chunkID, err)
	}
	return data, nil
}

// readFileSlice reads length bytes at offset from the named file.
func readFileSlice(path string, offset, length int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// writeAtomic writes data to path using a temp-file + rename, so readers
// never observe a partial write. Any existing file at path is replaced.
func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("cache write tmp %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("cache rename to %s: %w", path, err)
	}
	return nil
}
