package chunks

import (
	"bytes"
	"context"
	"testing"
)

func TestRecoverNextSeqEmpty(t *testing.T) {
	// No existing chunks: nextSeq and activeID must be unchanged.
	ctx := context.Background()
	local, _ := NewLocalStore(t.TempDir())
	cs := NewChunkStore(local, 1024)

	wantID := cs.activeID // "chunk-00000000"
	if err := cs.RecoverNextSeq(ctx); err != nil {
		t.Fatalf("RecoverNextSeq: %v", err)
	}
	if cs.activeID != wantID {
		t.Errorf("activeID changed on empty store: got %q, want %q", cs.activeID, wantID)
	}
	if cs.nextSeq != 1 {
		t.Errorf("nextSeq changed on empty store: got %d, want 1", cs.nextSeq)
	}
}

func TestRecoverNextSeqWithChunks(t *testing.T) {
	// Existing chunks 0, 1, 2: new active must be chunk-00000003.
	ctx := context.Background()
	local, _ := NewLocalStore(t.TempDir())

	// Populate the store directly so we can control which IDs exist.
	for _, id := range []string{"chunk-00000000", "chunk-00000001", "chunk-00000002"} {
		local.Put(ctx, id, []byte("data"))
	}

	cs := NewChunkStore(local, 1024)
	if err := cs.RecoverNextSeq(ctx); err != nil {
		t.Fatalf("RecoverNextSeq: %v", err)
	}
	if cs.activeID != "chunk-00000003" {
		t.Errorf("activeID: got %q, want %q", cs.activeID, "chunk-00000003")
	}
	if cs.nextSeq != 4 {
		t.Errorf("nextSeq: got %d, want 4", cs.nextSeq)
	}
}

func TestRecoverNextSeqGap(t *testing.T) {
	// Only chunk-00000005 exists (chunks 0-4 were compacted away):
	// new active must be chunk-00000006.
	ctx := context.Background()
	local, _ := NewLocalStore(t.TempDir())
	local.Put(ctx, "chunk-00000005", []byte("data"))

	cs := NewChunkStore(local, 1024)
	if err := cs.RecoverNextSeq(ctx); err != nil {
		t.Fatalf("RecoverNextSeq: %v", err)
	}
	if cs.activeID != "chunk-00000006" {
		t.Errorf("activeID: got %q, want %q", cs.activeID, "chunk-00000006")
	}
}

func TestRecoverNextSeqIgnoresNonChunkIDs(t *testing.T) {
	// Objects whose names don't match chunk-%08d must not influence nextSeq.
	ctx := context.Background()
	local, _ := NewLocalStore(t.TempDir())
	local.Put(ctx, "index.json", []byte("{}"))
	local.Put(ctx, "compacted-1234567890-0", []byte("data"))
	local.Put(ctx, "chunk-00000003", []byte("data")) // only this one counts

	cs := NewChunkStore(local, 1024)
	if err := cs.RecoverNextSeq(ctx); err != nil {
		t.Fatalf("RecoverNextSeq: %v", err)
	}
	if cs.activeID != "chunk-00000004" {
		t.Errorf("activeID: got %q, want %q", cs.activeID, "chunk-00000004")
	}
}

func TestRecoverNextSeqOutOfOrder(t *testing.T) {
	// List() may return IDs in any order; RecoverNextSeq must still find the max.
	ctx := context.Background()
	local, _ := NewLocalStore(t.TempDir())
	for _, id := range []string{"chunk-00000003", "chunk-00000001", "chunk-00000007", "chunk-00000002"} {
		local.Put(ctx, id, []byte("x"))
	}

	cs := NewChunkStore(local, 1024)
	if err := cs.RecoverNextSeq(ctx); err != nil {
		t.Fatalf("RecoverNextSeq: %v", err)
	}
	if cs.activeID != "chunk-00000008" {
		t.Errorf("activeID: got %q, want %q", cs.activeID, "chunk-00000008")
	}
}

// TestRecoverNextSeqPreventsActiveConflict is the end-to-end proof that
// RecoverNextSeq eliminates the cross-session ID collision.
//
// Without RecoverNextSeq: cs2.activeID = "chunk-00000000" which aliases the
// sealed chunk from cs1, causing cs2.Read to return "read out of bounds" on
// an empty active buffer.
//
// With RecoverNextSeq: cs2.activeID advances past all existing chunks so the
// read correctly falls through to the store (or cache).
func TestRecoverNextSeqPreventsActiveConflict(t *testing.T) {
	ctx := context.Background()
	local, _ := NewLocalStore(t.TempDir())

	// Session 1: write and seal a chunk — lands in the store as chunk-00000000.
	cs1 := NewChunkStore(local, 1024)
	data := []byte("session one data")
	ext, _ := cs1.Append(ctx, data, 0)
	cs1.Seal(ctx)

	if ext.ChunkID != "chunk-00000000" {
		t.Fatalf("unexpected chunk ID from session 1: %s", ext.ChunkID)
	}

	// Session 2: WITHOUT RecoverNextSeq, cs2.activeID = "chunk-00000000",
	// which collides with the sealed chunk. RecoverNextSeq must fix this.
	cs2 := NewChunkStore(local, 1024)
	if err := cs2.RecoverNextSeq(ctx); err != nil {
		t.Fatalf("RecoverNextSeq: %v", err)
	}

	if cs2.activeID == "chunk-00000000" {
		t.Error("RecoverNextSeq did not advance activeID past the sealed chunk")
	}

	// Reading the sealed chunk from session 1 must now succeed.
	got, err := cs2.Read(ctx, ext.ChunkID, ext.ChunkOffset, ext.Length)
	if err != nil {
		t.Fatalf("Read after RecoverNextSeq: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %q, want %q", got, data)
	}
}
