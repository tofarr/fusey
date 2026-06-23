package chunks

import (
	"bytes"
	"context"
	"testing"
)

func newTestStore(t *testing.T) (Store, *ChunkStore) {
	t.Helper()
	local, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return local, NewChunkStore(local, 1024) // 1 KiB chunks for tests
}

func TestAppendAndRead(t *testing.T) {
	ctx := context.Background()
	_, cs := newTestStore(t)

	data := []byte("hello, fusey!")
	ext, err := cs.Append(ctx, data, 0)
	if err != nil {
		t.Fatal(err)
	}
	if ext.Length != int64(len(data)) {
		t.Errorf("ext.Length: got %d, want %d", ext.Length, len(data))
	}
	if ext.FileOffset != 0 {
		t.Errorf("ext.FileOffset: got %d, want 0", ext.FileOffset)
	}

	got, err := cs.Read(ctx, ext.ChunkID, ext.ChunkOffset, ext.Length)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Read: got %q, want %q", got, data)
	}
}

func TestAppendMultiple(t *testing.T) {
	ctx := context.Background()
	_, cs := newTestStore(t)

	chunks := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}
	var fileOff int64
	var exts []interface{ chunkID() string }
	type extInfo struct {
		chunkID     string
		chunkOffset int64
		length      int64
		fileOffset  int64
	}
	var extInfos []extInfo
	for _, d := range chunks {
		ext, err := cs.Append(ctx, d, fileOff)
		if err != nil {
			t.Fatal(err)
		}
		extInfos = append(extInfos, extInfo{ext.ChunkID, ext.ChunkOffset, ext.Length, ext.FileOffset})
		fileOff += int64(len(d))
		_ = exts
	}

	// Read each extent back and verify content.
	offset := int64(0)
	for i, d := range chunks {
		ei := extInfos[i]
		got, err := cs.Read(ctx, ei.chunkID, ei.chunkOffset, ei.length)
		if err != nil {
			t.Fatalf("Read[%d]: %v", i, err)
		}
		if !bytes.Equal(got, d) {
			t.Errorf("Read[%d]: got %q, want %q", i, got, d)
		}
		offset += int64(len(d))
	}
}

// TestAutoRotate verifies that writing beyond maxChunkSize triggers a seal+rotate,
// and that both old and new chunk data are readable.
func TestAutoRotate(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// maxChunkSize = 20 bytes
	cs := NewChunkStore(local, 20)

	d1 := bytes.Repeat([]byte("A"), 15) // 15 bytes, fits in chunk-00000000
	d2 := bytes.Repeat([]byte("B"), 15) // 15 bytes, would overflow: rotate first

	ext1, err := cs.Append(ctx, d1, 0)
	if err != nil {
		t.Fatal(err)
	}
	ext2, err := cs.Append(ctx, d2, 15)
	if err != nil {
		t.Fatal(err)
	}

	// After rotation, chunks must be different.
	if ext1.ChunkID == ext2.ChunkID {
		t.Error("expected different chunk IDs after auto-rotate")
	}

	// Both extents must be readable.
	got1, err := cs.Read(ctx, ext1.ChunkID, ext1.ChunkOffset, ext1.Length)
	if err != nil {
		t.Fatalf("Read ext1: %v", err)
	}
	if !bytes.Equal(got1, d1) {
		t.Errorf("ext1 data: got %q, want %q", got1, d1)
	}

	got2, err := cs.Read(ctx, ext2.ChunkID, ext2.ChunkOffset, ext2.Length)
	if err != nil {
		t.Fatalf("Read ext2: %v", err)
	}
	if !bytes.Equal(got2, d2) {
		t.Errorf("ext2 data: got %q, want %q", got2, d2)
	}
}

// TestOnlyOneActiveChunk verifies the invariant from chunks.qnt:
// sealed chunks must exist in the store; the active chunk must not yet.
func TestOnlyOneActiveChunk(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cs := NewChunkStore(local, 10)

	d1 := []byte("0123456789") // exactly 10 bytes = maxChunkSize
	d2 := []byte("abcde")

	ext1, _ := cs.Append(ctx, d1, 0) // fills chunk-00000000
	_, _ = cs.Append(ctx, d2, 10)    // triggers rotate; d2 goes into chunk-00000001

	// chunk-00000000 (sealed) must exist in the store.
	sealed, err := local.Size(ctx, ext1.ChunkID)
	if err != nil {
		t.Fatalf("sealed chunk not in store: %v", err)
	}
	if sealed != 10 {
		t.Errorf("sealed chunk size: got %d, want 10", sealed)
	}

	// Active chunk (chunk-00000001) must NOT be in the store yet.
	activeID := cs.ActiveID()
	_, err = local.Size(ctx, activeID)
	if err == nil {
		t.Error("active chunk should not yet be in the store")
	}
}

func TestExplicitSeal(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cs := NewChunkStore(local, 1024)

	data := []byte("seal me")
	ext, _ := cs.Append(ctx, data, 0)
	oldActive := ext.ChunkID

	if err := cs.Seal(ctx); err != nil {
		t.Fatal(err)
	}

	newActive := cs.ActiveID()
	if newActive == oldActive {
		t.Error("active chunk ID should change after explicit seal")
	}

	// Old chunk must now be in the store.
	got, err := cs.Read(ctx, oldActive, 0, int64(len(data)))
	if err != nil {
		t.Fatalf("Read sealed chunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("sealed chunk data: got %q, want %q", got, data)
	}
}

func TestTooBigForChunk(t *testing.T) {
	ctx := context.Background()
	local, _ := NewLocalStore(t.TempDir())
	cs := NewChunkStore(local, 5) // 5-byte chunks

	_, err := cs.Append(ctx, []byte("way too big"), 0)
	if err == nil {
		t.Error("expected error appending data larger than maxChunkSize")
	}
}

// countingStore wraps a Store and records how many Put calls were made.
type countingStore struct {
	Store
	puts int
}

func (s *countingStore) Put(ctx context.Context, id string, data []byte) error {
	s.puts++
	return s.Store.Put(ctx, id, data)
}

// TestFlushActiveDirtyTracking verifies that FlushActive only issues a PUT
// when the active buffer has been modified since the last flush.
func TestFlushActiveDirtyTracking(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cs := NewChunkStore(local, 1024)
	cs.store = &countingStore{Store: local} // swap in counting store
	counting := cs.store.(*countingStore)

	// Fresh store: activeDirty is false, FlushActive must be a no-op.
	if cs.activeDirty {
		t.Error("activeDirty: want false before any append")
	}
	if err := cs.FlushActive(ctx); err != nil {
		t.Fatal(err)
	}
	if counting.puts != 0 {
		t.Errorf("FlushActive on clean buffer: want 0 PUTs, got %d", counting.puts)
	}

	// Append makes the buffer dirty.
	if _, err := cs.Append(ctx, []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if !cs.activeDirty {
		t.Error("activeDirty: want true after append")
	}

	// First FlushActive: should PUT and clear the dirty flag.
	if err := cs.FlushActive(ctx); err != nil {
		t.Fatal(err)
	}
	if cs.activeDirty {
		t.Error("activeDirty: want false after FlushActive")
	}
	putsAfterFirst := counting.puts

	// Second FlushActive with no new appends: must be a no-op.
	if err := cs.FlushActive(ctx); err != nil {
		t.Fatal(err)
	}
	if counting.puts != putsAfterFirst {
		t.Errorf("second FlushActive without append: want %d PUTs (no change), got %d",
			putsAfterFirst, counting.puts)
	}

	// Another append makes it dirty again.
	if _, err := cs.Append(ctx, []byte(" world"), 5); err != nil {
		t.Fatal(err)
	}
	if !cs.activeDirty {
		t.Error("activeDirty: want true after second append")
	}
	putsBeforeThird := counting.puts

	// Third FlushActive: should PUT again.
	if err := cs.FlushActive(ctx); err != nil {
		t.Fatal(err)
	}
	if counting.puts == putsBeforeThird {
		t.Error("third FlushActive after append: want a new PUT, got none")
	}
	if cs.activeDirty {
		t.Error("activeDirty: want false after third FlushActive")
	}

	// Seal clears the dirty flag too.
	if _, err := cs.Append(ctx, []byte("!"), 10); err != nil {
		t.Fatal(err)
	}
	if err := cs.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	if cs.activeDirty {
		t.Error("activeDirty: want false after Seal")
	}
}

// TestSealAfterFlushActive verifies that sealing a chunk after a partial
// FlushActive write produces the correct full buffer in the store.
// Store.Put has create-or-replace semantics, so sealLocked simply overwrites
// the partial flush with the complete buffer without a prior delete.
func TestSealAfterFlushActive(t *testing.T) {
	ctx := context.Background()
	_, cs := newTestStore(t)

	first := []byte("first write")
	ext, err := cs.Append(ctx, first, 0)
	if err != nil {
		t.Fatal(err)
	}
	chunkID := ext.ChunkID

	// Flush active — writes "first write" to the store but keeps the active buffer.
	if err := cs.FlushActive(ctx); err != nil {
		t.Fatal(err)
	}

	// Append more data into the same active chunk.
	second := []byte(" second write")
	if _, err := cs.Append(ctx, second, int64(len(first))); err != nil {
		t.Fatal(err)
	}

	// Seal — must overwrite the partial store version with the full buffer.
	if err := cs.Seal(ctx); err != nil {
		t.Fatalf("Seal after FlushActive: %v", err)
	}

	// The sealed chunk must contain the complete data (first + second).
	want := append(first, second...)
	got, err := cs.Read(ctx, chunkID, 0, int64(len(want)))
	if err != nil {
		t.Fatalf("Read sealed chunk: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("sealed chunk: got %q, want %q", got, want)
	}
}
