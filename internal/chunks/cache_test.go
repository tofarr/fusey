package chunks

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// spyStore wraps a Store and counts GetRange and Size calls so tests can
// verify whether reads are served from cache or the backing store.
type spyStore struct {
	Store
	getRangeCalls atomic.Int64
	sizeCalls     atomic.Int64
}

func (s *spyStore) GetRange(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	s.getRangeCalls.Add(1)
	return s.Store.GetRange(ctx, id, offset, length)
}

func (s *spyStore) Size(ctx context.Context, id string) (int64, error) {
	s.sizeCalls.Add(1)
	return s.Store.Size(ctx, id)
}

// newCachedTestStore builds a ChunkStore backed by a local spy store with
// chunk caching enabled. cacheDir is an isolated temp directory per test.
func newCachedTestStore(t *testing.T) (*ChunkStore, *spyStore) {
	t.Helper()
	local, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spy := &spyStore{Store: local}
	cs := NewChunkStore(spy, 1024)
	if err := cs.SetCacheDir(t.TempDir()); err != nil {
		t.Fatalf("SetCacheDir: %v", err)
	}
	return cs, spy
}

// TestCacheHitAfterSeal verifies that once a chunk is sealed, subsequent reads
// are served from the local cache and do not call the backing store.
func TestCacheHitAfterSeal(t *testing.T) {
	ctx := context.Background()
	cs, spy := newCachedTestStore(t)

	data := bytes.Repeat([]byte("hello"), 10) // 50 bytes
	ext, err := cs.Append(ctx, data, 0)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Seal so the chunk is written to the store and the local cache.
	if err := cs.Seal(ctx); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	storeCalls := spy.getRangeCalls.Load()

	// Read back the sealed chunk — should hit the local cache.
	got, err := cs.Read(ctx, ext.ChunkID, ext.ChunkOffset, ext.Length)
	if err != nil {
		t.Fatalf("Read after seal: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %q, want %q", got, data)
	}
	if spy.getRangeCalls.Load() != storeCalls {
		t.Errorf("Read after seal called store.GetRange %d extra time(s); want 0 (should be cache hit)",
			spy.getRangeCalls.Load()-storeCalls)
	}
}

// TestCacheMissFetchesFullChunk verifies that when a sealed chunk is not in
// the local cache, the full chunk is fetched from the store exactly once, then
// cached so the second read does not touch the store.
func TestCacheMissFetchesFullChunk(t *testing.T) {
	ctx := context.Background()

	local, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spy := &spyStore{Store: local}

	// cs1: write and seal without a cache so the chunk lands in the store only.
	cs1 := NewChunkStore(spy, 1024)
	target := bytes.Repeat([]byte("ab"), 20) // 40 bytes
	ext, _ := cs1.Append(ctx, target, 0)
	cs1.Seal(ctx)

	// cs2: fresh ChunkStore + cache. RecoverNextSeq advances its activeID
	// past chunk-00000000, preventing the ID collision that would otherwise
	// cause Read to hit the empty active buffer instead of the store.
	cs2 := NewChunkStore(spy, 1024)
	if err := cs2.RecoverNextSeq(ctx); err != nil {
		t.Fatalf("RecoverNextSeq: %v", err)
	}
	if err := cs2.SetCacheDir(t.TempDir()); err != nil {
		t.Fatalf("SetCacheDir: %v", err)
	}

	spy.getRangeCalls.Store(0)

	// First read: cache miss — should fetch from store (1 GetRange call).
	got, err := cs2.Read(ctx, ext.ChunkID, ext.ChunkOffset, ext.Length)
	if err != nil {
		t.Fatalf("first Read: %v", err)
	}
	if !bytes.Equal(got, target) {
		t.Errorf("first read data mismatch: got %q, want %q", got, target)
	}
	if spy.getRangeCalls.Load() != 1 {
		t.Errorf("expected 1 GetRange call on cache miss, got %d", spy.getRangeCalls.Load())
	}

	spy.getRangeCalls.Store(0)

	// Second read: cache hit — store must not be called again.
	got2, err := cs2.Read(ctx, ext.ChunkID, 0, 10) // partial range
	if err != nil {
		t.Fatalf("second Read: %v", err)
	}
	if !bytes.Equal(got2, target[:10]) {
		t.Errorf("second read data mismatch: got %q, want %q", got2, target[:10])
	}
	if spy.getRangeCalls.Load() != 0 {
		t.Errorf("second read should be a cache hit (0 store calls), got %d", spy.getRangeCalls.Load())
	}
}

// TestCacheConcurrentMissDeduplicates fires many goroutines reading the same
// uncached chunk simultaneously and verifies the backing store is called
// exactly once (inflight deduplication). Uses chunk-00000001 as the target
// to avoid active-ID collision with a fresh ChunkStore (see
// TestCacheMissFetchesFullChunk for the explanation).
func TestCacheConcurrentMissDeduplicates(t *testing.T) {
	ctx := context.Background()

	local, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spy := &spyStore{Store: local}

	cs1 := NewChunkStore(spy, 1024)
	data := bytes.Repeat([]byte("y"), 100)
	ext, _ := cs1.Append(ctx, data, 0)
	cs1.Seal(ctx)

	// cs2: RecoverNextSeq prevents the active-ID collision so all 50 goroutines
	// correctly route to the cache/store path rather than the active buffer.
	cs2 := NewChunkStore(spy, 1024)
	if err := cs2.RecoverNextSeq(ctx); err != nil {
		t.Fatalf("RecoverNextSeq: %v", err)
	}
	if err := cs2.SetCacheDir(t.TempDir()); err != nil {
		t.Fatalf("SetCacheDir: %v", err)
	}
	spy.getRangeCalls.Store(0)

	const n = 50
	results := make([][]byte, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = cs2.Read(ctx, ext.ChunkID, ext.ChunkOffset, ext.Length)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d: Read error: %v", i, e)
		}
	}
	for i, got := range results {
		if !bytes.Equal(got, data) {
			t.Errorf("goroutine %d: data mismatch", i)
		}
	}
	if calls := spy.getRangeCalls.Load(); calls != 1 {
		t.Errorf("expected exactly 1 GetRange call for concurrent misses, got %d", calls)
	}
}

// TestCacheReadPartialRanges verifies that different sub-ranges of a cached
// chunk all return correct data slices.
func TestCacheReadPartialRanges(t *testing.T) {
	ctx := context.Background()
	cs, _ := newCachedTestStore(t)

	// Build a recognisable 200-byte payload.
	data := make([]byte, 200)
	for i := range data {
		data[i] = byte(i % 256)
	}
	ext, _ := cs.Append(ctx, data, 0)
	cs.Seal(ctx)

	cases := []struct{ off, length int64 }{
		{0, 1},
		{0, 200},
		{10, 50},
		{199, 1},
		{100, 100},
	}
	for _, c := range cases {
		got, err := cs.Read(ctx, ext.ChunkID, c.off, c.length)
		if err != nil {
			t.Errorf("Read(%d,%d): %v", c.off, c.length, err)
			continue
		}
		want := data[c.off : c.off+c.length]
		if !bytes.Equal(got, want) {
			t.Errorf("Read(%d,%d): data mismatch", c.off, c.length)
		}
	}
}

// TestCachePersistsAcrossChunkStoreRestart simulates a pod restart: a chunk
// sealed in the first ChunkStore session should be served from the on-disk
// cache in the second session without calling the backing store.
func TestCachePersistsAcrossChunkStoreRestart(t *testing.T) {
	ctx := context.Background()

	local, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spy := &spyStore{Store: local}
	cacheDir := t.TempDir()

	// Session 1: write and seal — chunk lands in the store and the local cache.
	cs1 := NewChunkStore(spy, 1024)
	if err := cs1.SetCacheDir(cacheDir); err != nil {
		t.Fatalf("SetCacheDir: %v", err)
	}
	data := []byte("persistent data across restarts")
	ext, _ := cs1.Append(ctx, data, 0)
	cs1.Seal(ctx)

	// Session 2: fresh ChunkStore, same cache dir. RecoverNextSeq advances
	// activeID so chunk-00000000 is not mistaken for the new empty active chunk.
	spy.getRangeCalls.Store(0)
	cs2 := NewChunkStore(spy, 1024)
	if err := cs2.RecoverNextSeq(ctx); err != nil {
		t.Fatalf("RecoverNextSeq: %v", err)
	}
	if err := cs2.SetCacheDir(cacheDir); err != nil {
		t.Fatalf("SetCacheDir session 2: %v", err)
	}

	got, err := cs2.Read(ctx, ext.ChunkID, ext.ChunkOffset, ext.Length)
	if err != nil {
		t.Fatalf("Read after restart: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data after restart: got %q, want %q", got, data)
	}
	if spy.getRangeCalls.Load() != 0 {
		t.Errorf("Read after restart called store.GetRange %d time(s); want 0 (cache persists on disk)",
			spy.getRangeCalls.Load())
	}
}

// TestSetCacheDirCreatesDirectory verifies that SetCacheDir creates the
// chunks subdirectory if it does not already exist.
func TestSetCacheDirCreatesDirectory(t *testing.T) {
	local, _ := NewLocalStore(t.TempDir())
	cs := NewChunkStore(local, 1024)
	baseDir := t.TempDir() + "/nonexistent/nested"
	if err := cs.SetCacheDir(baseDir); err != nil {
		t.Fatalf("SetCacheDir with non-existent path: %v", err)
	}
}
