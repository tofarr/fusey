package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tofarr/fusey/internal/chunks"
	"github.com/tofarr/fusey/internal/index"
)

// --- Test helpers ---

// newTestStore returns a LocalStore-backed ObjectStore in a temp dir.
// LocalStore does not implement ObjectStore (no PutRaw/GetRaw/IndexKey),
// so we use chunks.NewLocalStore and then build a small wrapper that
// exposes the ObjectStore interface. We do this inline rather than via
// a helper in the chunks package to keep this test self-contained.
func newTestStore(t *testing.T) chunks.ObjectStore {
	t.Helper()
	dir := t.TempDir()
	ls, err := chunks.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("local store: %v", err)
	}
	return &testObjectStore{Store: ls, prefix: ""}
}

// testObjectStore adapts a chunks.Store (LocalStore) to ObjectStore by
// adding the PutRaw/GetRaw/IndexKey methods LocalStore doesn't implement.
type testObjectStore struct {
	chunks.Store
	prefix string
}

func (s *testObjectStore) PutRaw(ctx context.Context, key string, data []byte) error {
	return s.Store.Put(ctx, key, data)
}

func (s *testObjectStore) GetRaw(ctx context.Context, key string) ([]byte, error) {
	// Use a range-GET of the full file size to read the bytes back.
	size, err := s.Store.Size(ctx, key)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, chunks.ErrNotFound
	}
	return s.Store.GetRange(ctx, key, 0, size)
}

func (s *testObjectStore) IndexKey() string   { return s.prefix + "index.cbor" }
func (s *testObjectStore) Prefix() string     { return s.prefix }
func (s *testObjectStore) JournalKey(seq uint64) string {
	return s.prefix + chunks.JournalShardName(seq)
}
func (s *testObjectStore) DeleteJournalKey(ctx context.Context, key string) error {
	return s.Store.Delete(ctx, key)
}

// --- Tests ---

func TestJournalAppendAssignsSeq(t *testing.T) {
	j := New(true)
	seq1 := j.Append(Op{SetXattr: &SetXattr{Ino: 1, Name: "x", Value: "v"}}, 100)
	seq2 := j.Append(Op{SetXattr: &SetXattr{Ino: 1, Name: "y", Value: "v"}}, 200)
	if seq1 != 1 {
		t.Errorf("first seq: got %d, want 1", seq1)
	}
	if seq2 != 2 {
		t.Errorf("second seq: got %d, want 2", seq2)
	}
	if j.BufferLen() != 2 {
		t.Errorf("buffer len: got %d, want 2", j.BufferLen())
	}
}

func TestJournalDisabledIsNoop(t *testing.T) {
	j := New(false)
	seq := j.Append(Op{SetXattr: &SetXattr{Ino: 1, Name: "x", Value: "v"}}, 100)
	if seq != 0 {
		t.Errorf("disabled append: got seq %d, want 0", seq)
	}
	if j.BufferLen() != 0 {
		t.Errorf("disabled buffer len: got %d, want 0", j.BufferLen())
	}
	if j.IsDirty() {
		t.Error("disabled journal should not be dirty")
	}
}

func TestJournalFlushAndLoad(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	j := New(true)

	// Append several events spanning multiple ops.
	ts := int64(1_000_000_000)
	j.Append(Op{CreateInode: &CreateInode{Ino: 2, FileType: index.Regular, Mode: 0o644, Nlink: 1, UID: 1000, GID: 1000, Atime: ts, Mtime: ts, Ctime: ts, Blksize: 4096}}, ts)
	j.Append(Op{AddDirEntry: &AddDirEntry{ParentIno: 1, Name: "hello.txt", ChildIno: 2}}, ts+1)
	j.Append(Op{SetXattr: &SetXattr{Ino: 2, Name: "user.author", Value: "alice"}}, ts+2)
	if !j.IsDirty() {
		t.Fatal("journal should be dirty after appends")
	}

	shardSeq, count, err := Flush(ctx, j, store)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if shardSeq != 1 {
		t.Errorf("shard seq: got %d, want 1", shardSeq)
	}
	// Diagnostic removed; tests run cleanly once key construction is right.
	_ = shardSeq
	if count != 3 {
		t.Errorf("entry count: got %d, want 3", count)
	}
	if j.IsDirty() {
		t.Error("journal should not be dirty after flush")
	}
	if j.BufferLen() != 0 {
		t.Errorf("buffer len after flush: got %d, want 0", j.BufferLen())
	}

	// Load and verify.
	loaded, err := LoadAll(ctx, store)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 3 {
		t.Fatalf("loaded len: got %d, want 3", len(loaded))
	}
	if loaded[0].Seq != 1 || loaded[0].Ts != ts {
		t.Errorf("first entry: seq=%d ts=%d", loaded[0].Seq, loaded[0].Ts)
	}
	if loaded[0].Op.CreateInode == nil || loaded[0].Op.CreateInode.Ino != 2 {
		t.Errorf("first entry: CreateInode not as expected: %+v", loaded[0].Op)
	}
	if loaded[1].Op.AddDirEntry == nil || loaded[1].Op.AddDirEntry.Name != "hello.txt" {
		t.Errorf("second entry: AddDirEntry not as expected: %+v", loaded[1].Op)
	}
	if loaded[2].Op.SetXattr == nil || loaded[2].Op.SetXattr.Name != "user.author" {
		t.Errorf("third entry: SetXattr not as expected: %+v", loaded[2].Op)
	}
}

func TestJournalMultipleShardsOrderedBySeq(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	j := New(true)
	for i := 0; i < 5; i++ {
		j.Append(Op{SetXattr: &SetXattr{Ino: 1, Name: "k", Value: "v"}}, int64(i))
	}
	if _, _, err := Flush(ctx, j, store); err != nil {
		t.Fatalf("flush 1: %v", err)
	}
	// Next batch will start at seq 6.
	for i := 0; i < 3; i++ {
		j.Append(Op{SetXattr: &SetXattr{Ino: 1, Name: "k2", Value: "v"}}, int64(100+i))
	}
	if _, _, err := Flush(ctx, j, store); err != nil {
		t.Fatalf("flush 2: %v", err)
	}

	loaded, err := LoadAll(ctx, store)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 8 {
		t.Fatalf("loaded len: got %d, want 8", len(loaded))
	}
	for i, e := range loaded {
		if e.Seq != uint64(i+1) {
			t.Errorf("entry %d: seq=%d, want %d", i, e.Seq, i+1)
		}
	}
}

func TestJournalClearRemovesShards(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	j := New(true)
	j.Append(Op{SetXattr: &SetXattr{Ino: 1, Name: "k", Value: "v"}}, 1)
	if _, _, err := Flush(ctx, j, store); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Confirm a shard exists.
	loaded, _ := LoadAll(ctx, store)
	if len(loaded) == 0 {
		t.Fatal("expected a shard to exist")
	}
	if err := Clear(ctx, store, ""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	// Confirm shards are gone.
	loaded, _ = LoadAll(ctx, store)
	if len(loaded) != 0 {
		t.Errorf("expected no shards after clear, got %d", len(loaded))
	}
}

func TestApplyReplayMatchesLiveOps(t *testing.T) {
	// Build an index with a fresh root.
	idx := index.New(4096)
	now := time.Now().UnixNano()
	var err error

	// Apply operations directly to idx (live).
	_, err = idx.CreateInode(index.Directory, 0o755, 0, 0, 0, now)
	must(t, err)
	dirIno := uint64(2)
	must(t, idx.AddDirEntry(1 /*root*/, "subdir", dirIno, now+1))
	_, err = idx.CreateInode(index.Regular, 0o644, 1000, 1000, 0, now+2)
	must(t, err)
	fileIno := uint64(3)
	must(t, idx.AddDirEntry(dirIno, "file.txt", fileIno, now+3))
	must(t, idx.WriteExtent(fileIno, index.Extent{
		ChunkID: "chunk-00000000", ChunkOffset: 0, Length: 5, FileOffset: 0,
	}, now+4))
	must(t, idx.SetXattr(fileIno, "user.tag", "v1", now+5))
	_ = idx

	// Build the equivalent journal of ops.
	entries := []Entry{
		{Seq: 1, Ts: now, Op: Op{CreateInode: &CreateInode{Ino: 2, FileType: index.Directory, Mode: 0o755, Nlink: 0, Atime: now, Mtime: now, Ctime: now, Blksize: 4096}}},
		{Seq: 2, Ts: now + 1, Op: Op{AddDirEntry: &AddDirEntry{ParentIno: 1, Name: "subdir", ChildIno: 2}}},
		{Seq: 3, Ts: now + 2, Op: Op{CreateInode: &CreateInode{Ino: 3, FileType: index.Regular, Mode: 0o644, Nlink: 0, UID: 1000, GID: 1000, Atime: now + 2, Mtime: now + 2, Ctime: now + 2, Blksize: 4096}}},
		{Seq: 4, Ts: now + 3, Op: Op{AddDirEntry: &AddDirEntry{ParentIno: 2, Name: "file.txt", ChildIno: 3}}},
		{Seq: 5, Ts: now + 4, Op: Op{WriteExtent: &WriteExtent{Ino: 3, Extent: index.Extent{ChunkID: "chunk-00000000", ChunkOffset: 0, Length: 5, FileOffset: 0}}}},
		{Seq: 6, Ts: now + 5, Op: Op{SetXattr: &SetXattr{Ino: 3, Name: "user.tag", Value: "v1"}}},
	}

	// The live index's state is the "expected" state. Now replay the
	// journal into a fresh index and compare.
	freshBase := index.New(4096)
	replayed, err := ReplayUpTo(freshBase, entries, now+10)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	// Compare extents.
	liveExts, _ := idx.GetExtents(fileIno)
	repExts, _ := replayed.GetExtents(fileIno)
	if len(liveExts) != len(repExts) {
		t.Fatalf("extent count mismatch: live=%d replay=%d", len(liveExts), len(repExts))
	}
	for i := range liveExts {
		if liveExts[i] != repExts[i] {
			t.Errorf("extent %d: live=%+v replay=%+v", i, liveExts[i], repExts[i])
		}
	}
	// Compare xattr.
	if got := replayed.GetXattrs(fileIno)["user.tag"]; got != "v1" {
		t.Errorf("xattr after replay: got %q, want v1", got)
	}
	// Compare dirEntry.
	if got, ok := replayed.Lookup(2, "file.txt"); !ok || got != fileIno {
		t.Errorf("dirEntry after replay: got (ino=%d, ok=%v), want (%d, true)", got, ok, fileIno)
	}
}

func TestReplayUpToFiltersByTimestamp(t *testing.T) {
	base := index.New(4096)
	now := int64(1_000_000_000)
	entries := []Entry{
		{Seq: 1, Ts: now, Op: Op{CreateInode: &CreateInode{Ino: 2, FileType: index.Regular, Mode: 0o644, Nlink: 0, Blksize: 4096, Atime: now, Mtime: now, Ctime: now}}},
		{Seq: 2, Ts: now + 100, Op: Op{AddDirEntry: &AddDirEntry{ParentIno: 1, Name: "a", ChildIno: 2}}},
		{Seq: 3, Ts: now + 200, Op: Op{SetXattr: &SetXattr{Ino: 2, Name: "k", Value: "v"}}},
		{Seq: 4, Ts: now + 300, Op: Op{SetXattr: &SetXattr{Ino: 2, Name: "k2", Value: "v2"}}},
	}

	// Replay to now+150: only entries 1, 2 should apply.
	idx, err := ReplayUpTo(base, entries, now+150)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got := idx.GetXattrs(2); len(got) != 0 {
		t.Errorf("at now+150, xattrs should be empty: got %+v", got)
	}
	// dirEntry for a/2 should be present.
	if _, ok := idx.Lookup(1, "a"); !ok {
		t.Error("dirEntry for 'a' should be present at now+150")
	}

	// Replay to now+250: entries 1, 2, 3 apply.
	idx2, err := ReplayUpTo(base, entries, now+250)
	if err != nil {
		t.Fatalf("replay 2: %v", err)
	}
	if got := idx2.GetXattrs(2)["k"]; got != "v" {
		t.Errorf("at now+250, xattr k: got %q, want v", got)
	}
	if got := idx2.GetXattrs(2)["k2"]; got != "" {
		t.Errorf("at now+250, xattr k2 should not exist: got %q", got)
	}
}

func TestJournaledIndexPairsEveryMutationWithAppend(t *testing.T) {
	idx := index.New(4096)
	j := New(true)
	ji := Wrap(idx, j)
	now := time.Now().UnixNano()

	// Drive a few mutations through the wrapper.
	ino, err := ji.CreateInode(index.Regular, 0o644, 0, 0, 0, now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ji.AddDirEntry(1, "x", ino, now+1); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := ji.WriteExtent(ino, index.Extent{ChunkID: "chunk-00000000", ChunkOffset: 0, Length: 5, FileOffset: 0}, now+2); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := ji.SetXattr(ino, "k", "v", now+3); err != nil {
		t.Fatalf("setxattr: %v", err)
	}

	// 4 ops recorded.
	if got := j.BufferLen(); got != 4 {
		t.Errorf("buffer len: got %d, want 4", got)
	}
	// All seqs 1..4.
	for i, e := range j.buffer {
		if e.Seq != uint64(i+1) {
			t.Errorf("entry %d seq: got %d, want %d", i, e.Seq, i+1)
		}
	}
}

func TestJournaledIndexDisabledIsNoopPassThrough(t *testing.T) {
	idx := index.New(4096)
	j := New(false)
	ji := Wrap(idx, j)
	now := time.Now().UnixNano()

	// Mutations go through but no journal entries are recorded.
	ino, err := ji.CreateInode(index.Regular, 0o644, 0, 0, 0, now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ji.AddDirEntry(1, "x", ino, now+1); err != nil {
		t.Fatalf("add: %v", err)
	}
	if got := j.BufferLen(); got != 0 {
		t.Errorf("disabled journal: got buffer len %d, want 0", got)
	}
	// Index state is correct.
	if got, ok := idx.Lookup(1, "x"); !ok || got != ino {
		t.Errorf("after pass-through: dirEntry (ino=%d, ok=%v), want (%d, true)", got, ok, ino)
	}
}

func TestJournaledIndexRemoveDirEntryEmitsDeleteInodeOnNlinkZero(t *testing.T) {
	idx := index.New(4096)
	j := New(true)
	ji := Wrap(idx, j)
	now := time.Now().UnixNano()
	// Create a file with nlink=1, then remove its only dirEntry.
	ino, err := ji.CreateInode(index.Regular, 0o644, 0, 0, 0, now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ji.AddDirEntry(1, "x", ino, now+1); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Reset buffer to focus on what RemoveDirEntry appends.
	before := j.BufferLen()
	if err := ji.RemoveDirEntry(1, "x", now+2); err != nil {
		t.Fatalf("remove: %v", err)
	}
	after := j.BufferLen()
	if got := after - before; got != 2 {
		t.Errorf("expected 2 new entries (RemoveDirEntry + DeleteInode), got %d", got)
	}
	if j.buffer[before].Op.RemoveDirEntry == nil {
		t.Errorf("first new op should be RemoveDirEntry: %+v", j.buffer[before].Op)
	}
	if j.buffer[before+1].Op.DeleteInode == nil || j.buffer[before+1].Op.DeleteInode.Ino != ino {
		t.Errorf("second new op should be DeleteInode for ino=%d: %+v", ino, j.buffer[before+1].Op)
	}
}

func TestJournaledIndexRenameEmitsDeleteInodeOnReplace(t *testing.T) {
	idx := index.New(4096)
	j := New(true)
	ji := Wrap(idx, j)
	now := time.Now().UnixNano()

	// Create src and dst files, both with nlink=1.
	srcIno, _ := ji.CreateInode(index.Regular, 0o644, 0, 0, 0, now)
	dstIno, _ := ji.CreateInode(index.Regular, 0o644, 0, 0, 0, now+1)
	_ = ji.AddDirEntry(1, "src", srcIno, now+2)
	_ = ji.AddDirEntry(1, "dst", dstIno, now+3)
	before := j.BufferLen()

	// Rename src -> dst. dst should be replaced (and freed).
	if err := ji.Rename(1, "src", 1, "dst", now+4); err != nil {
		t.Fatalf("rename: %v", err)
	}
	after := j.BufferLen()
	if got := after - before; got != 2 {
		t.Errorf("expected 2 new entries (Rename + DeleteInode), got %d", got)
	}
	if j.buffer[before].Op.Rename == nil {
		t.Errorf("first new op should be Rename: %+v", j.buffer[before].Op)
	}
	if j.buffer[before+1].Op.DeleteInode == nil || j.buffer[before+1].Op.DeleteInode.Ino != dstIno {
		t.Errorf("second new op should be DeleteInode for dstIno=%d: %+v", dstIno, j.buffer[before+1].Op)
	}
}

func TestDumpHumanReadable(t *testing.T) {
	entries := []Entry{
		{Seq: 1, Ts: 1700000000000000000, Op: Op{CreateInode: &CreateInode{Ino: 2, FileType: index.Regular, Mode: 0o644, Nlink: 1, UID: 1000, GID: 1000, Blksize: 4096, Atime: 1700000000000000000, Mtime: 1700000000000000000, Ctime: 1700000000000000000}}},
		{Seq: 2, Ts: 1700000000000000001, Op: Op{AddDirEntry: &AddDirEntry{ParentIno: 1, Name: "hello world.txt", ChildIno: 2}}},
		{Seq: 3, Ts: 1700000000000000002, Op: Op{SetXattr: &SetXattr{Ino: 2, Name: "user.author", Value: "alice"}}},
	}
	var buf bytes.Buffer
	if err := Dump(&buf, entries); err != nil {
		t.Fatalf("dump: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "op=CreateInode") {
		t.Errorf("output missing CreateInode: %s", out)
	}
	if !strings.Contains(out, "ino=2") {
		t.Errorf("output missing ino=2: %s", out)
	}
	if !strings.Contains(out, "op=AddDirEntry") {
		t.Errorf("output missing AddDirEntry: %s", out)
	}
	if !strings.Contains(out, `"hello world.txt"`) {
		t.Errorf("output missing quoted name: %s", out)
	}
	if !strings.Contains(out, "op=SetXattr") {
		t.Errorf("output missing SetXattr: %s", out)
	}
	if !strings.Contains(out, "name=user.author") {
		t.Errorf("output missing xattr name: %s", out)
	}
	if !strings.Contains(out, "value=alice") {
		t.Errorf("output missing xattr value: %s", out)
	}
}

func TestDumpJSON(t *testing.T) {
	entries := []Entry{
		{Seq: 1, Ts: 1700000000000000000, Op: Op{CreateInode: &CreateInode{Ino: 2, FileType: index.Regular, Mode: 0o644, Nlink: 1, UID: 1000, GID: 1000, Blksize: 4096, Atime: 1700000000000000000, Mtime: 1700000000000000000, Ctime: 1700000000000000000}}},
	}
	var buf bytes.Buffer
	if err := DumpJSON(&buf, entries); err != nil {
		t.Fatalf("dump json: %v", err)
	}
	var parsed []map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(parsed) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(parsed))
	}
	if parsed[0]["seq"].(float64) != 1 {
		t.Errorf("seq: got %v, want 1", parsed[0]["seq"])
	}
	if parsed[0]["ts"].(float64) != 1700000000000000000 {
		t.Errorf("ts: got %v", parsed[0]["ts"])
	}
	op := parsed[0]["op"].(map[string]interface{})
	if op["kind"] != "CreateInode" {
		t.Errorf("op kind: got %v, want CreateInode", op["kind"])
	}
	fields := op["fields"].(map[string]interface{})
	if fields["ino"].(float64) != 2 {
		t.Errorf("ino: got %v, want 2", fields["ino"])
	}
}

func TestSetNextSeqAfter(t *testing.T) {
	j := New(true)
	es := []Entry{
		{Seq: 1, Ts: 100, Op: Op{SetXattr: &SetXattr{Ino: 1, Name: "k", Value: "v"}}},
		{Seq: 2, Ts: 200, Op: Op{SetXattr: &SetXattr{Ino: 1, Name: "k", Value: "v"}}},
		{Seq: 3, Ts: 300, Op: Op{SetXattr: &SetXattr{Ino: 1, Name: "k", Value: "v"}}},
	}
	if err := j.SetNextSeqAfter(es); err != nil {
		t.Fatalf("set next: %v", err)
	}
	// Append should now assign seq=4.
	seq := j.Append(Op{SetXattr: &SetXattr{Ino: 1, Name: "k", Value: "v"}}, 400)
	if seq != 4 {
		t.Errorf("after SetNextSeqAfter: got seq=%d, want 4", seq)
	}
	// Buffer should be empty (the loaded entries aren't appended).
	if got := j.BufferLen(); got != 1 {
		t.Errorf("buffer len after SetNextSeqAfter + 1 append: got %d, want 1", got)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
