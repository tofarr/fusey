package index

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func now() int64 { return time.Now().UnixNano() }

// checkInvariants verifies the key invariants from index.qnt against idx.
// It is called after every mutating operation in the tests.
func checkInvariants(t *testing.T, idx *Index) {
	t.Helper()
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// rootExists + rootIsDir
	root, ok := idx.inodes[RootIno]
	if !ok {
		t.Error("invariant rootExists: root inode missing")
		return
	}
	if root.FileType != Directory {
		t.Error("invariant rootIsDir: root is not a directory")
	}

	// inodeKeyConsistency
	for ino, inode := range idx.inodes {
		if inode.Ino != ino {
			t.Errorf("invariant inodeKeyConsistency: inodes[%d].Ino = %d", ino, inode.Ino)
		}
	}

	// dirEntriesValid + parentIsDir
	for parentIno, children := range idx.dirIndex {
		parent, ok := idx.inodes[parentIno]
		if !ok {
			t.Errorf("invariant dirEntriesValid: parent inode %d missing", parentIno)
			continue
		}
		if parent.FileType != Directory {
			t.Errorf("invariant parentIsDir: inode %d is parent but not a directory", parentIno)
		}
		for name, childIno := range children {
			if _, ok := idx.inodes[childIno]; !ok {
				t.Errorf("invariant dirEntriesValid: child inode %d (name=%q, parent=%d) missing",
					childIno, name, parentIno)
			}
		}
	}

	// namesUnique is guaranteed by the map structure (enforced by Go map keys).

	// nlinkConsistency
	refCount := make(map[uint64]uint32)
	for _, children := range idx.dirIndex {
		for _, childIno := range children {
			refCount[childIno]++
		}
	}
	for ino, inode := range idx.inodes {
		extra := uint32(0)
		if inode.FileType == Directory {
			extra = 1 // directory counts itself ("." link)
		}
		expected := refCount[ino] + extra
		if inode.Nlink != expected {
			t.Errorf("invariant nlinkConsistency: inode %d Nlink=%d, expected=%d",
				ino, inode.Nlink, expected)
		}
	}

	// extentSizeConsistency
	for ino, inode := range idx.inodes {
		if inode.FileType != Regular {
			continue
		}
		exts := idx.extents[ino]
		var total int64
		for _, e := range exts {
			total += e.Length
		}
		if total != inode.Size {
			t.Errorf("invariant extentSizeConsistency: inode %d size=%d, extentTotal=%d",
				ino, inode.Size, total)
		}
	}

	// sizeNonNeg
	for ino, inode := range idx.inodes {
		if inode.Size < 0 {
			t.Errorf("invariant sizeNonNeg: inode %d size=%d", ino, inode.Size)
		}
	}

	// nextInoMonotone
	for ino := range idx.inodes {
		if ino >= idx.nextIno {
			t.Errorf("invariant nextInoMonotone: inode %d >= nextIno %d", ino, idx.nextIno)
		}
	}

	// symlinkConsistency
	for ino := range idx.symlinks {
		inode, ok := idx.inodes[ino]
		if !ok {
			t.Errorf("invariant symlinkConsistency: symlink target for missing inode %d", ino)
			continue
		}
		if inode.FileType != Symlink {
			t.Errorf("invariant symlinkConsistency: inode %d has symlink target but fileType=%v", ino, inode.FileType)
		}
	}

	// blksizeConsistency
	for ino, inode := range idx.inodes {
		if inode.Blksize != idx.blockSize {
			t.Errorf("invariant blksizeConsistency: inode %d Blksize=%d, want %d",
				ino, inode.Blksize, idx.blockSize)
		}
	}

	// noOrphanedInodes: every inode in the map must have at least one
	// directory reference (nlink >= 1). An nlink=0 inode has no directory
	// entry and is unreachable — a leaked orphan from a failed two-phase
	// create (CreateInode succeeded, AddDirEntry failed).
	for ino, inode := range idx.inodes {
		if inode.Nlink == 0 {
			t.Errorf("invariant noOrphanedInodes: inode %d has nlink=0 (unreachable orphan)", ino)
		}
	}
}

// --- Tests ---

func TestNew(t *testing.T) {
	idx := New(4096)
	checkInvariants(t, idx)

	inode, ok := idx.GetInode(RootIno)
	if !ok {
		t.Fatal("root inode not found")
	}
	if inode.FileType != Directory {
		t.Errorf("root FileType: got %v, want Directory", inode.FileType)
	}
	if inode.Mode != 0o755 {
		t.Errorf("root Mode: got %o, want 755", inode.Mode)
	}
}

func TestCreateAndLookup(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, err := idx.CreateInode(Regular, 0o644, 1000, 1000, 0, n)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.AddDirEntry(RootIno, "hello.txt", ino, n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	got, ok := idx.Lookup(RootIno, "hello.txt")
	if !ok || got != ino {
		t.Errorf("Lookup: got %d, %v; want %d, true", got, ok, ino)
	}
}

func TestNlinkHardLink(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "a.txt", ino, n)
	checkInvariants(t, idx)

	// Add a second name for the same inode.
	if err := idx.AddDirEntry(RootIno, "b.txt", ino, n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	inode, _ := idx.GetInode(ino)
	if inode.Nlink != 2 {
		t.Errorf("Nlink after hard link: got %d, want 2", inode.Nlink)
	}
}

func TestUnlinkDecrements(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "a.txt", ino, n)
	idx.AddDirEntry(RootIno, "b.txt", ino, n)
	checkInvariants(t, idx)

	if err := idx.RemoveDirEntry(RootIno, "a.txt", n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	inode, ok := idx.GetInode(ino)
	if !ok {
		t.Fatal("inode should still exist after nlink=1")
	}
	if inode.Nlink != 1 {
		t.Errorf("Nlink: got %d, want 1", inode.Nlink)
	}

	// Remove last name: inode should be freed.
	if err := idx.RemoveDirEntry(RootIno, "b.txt", n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	if _, ok := idx.GetInode(ino); ok {
		t.Error("inode should have been freed after nlink=0")
	}
}

func TestWriteExtentCOW(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "f.bin", ino, n)

	// Write bytes 0–99.
	idx.WriteExtent(ino, Extent{ChunkID: "c0", ChunkOffset: 0, Length: 100, FileOffset: 0}, n)
	checkInvariants(t, idx)

	inode, _ := idx.GetInode(ino)
	if inode.Size != 100 {
		t.Errorf("size after first write: got %d, want 100", inode.Size)
	}

	// Overwrite bytes 50–74 (overlapping the first extent).
	idx.WriteExtent(ino, Extent{ChunkID: "c0", ChunkOffset: 200, Length: 25, FileOffset: 50}, n)
	checkInvariants(t, idx)

	// Extent total must still equal size.
	exts, _ := idx.GetExtents(ino)
	var total int64
	for _, e := range exts {
		total += e.Length
	}
	inode, _ = idx.GetInode(ino)
	if total != inode.Size {
		t.Errorf("extentTotal=%d != size=%d after overwrite", total, inode.Size)
	}
}

func TestTruncate(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "f.bin", ino, n)
	idx.WriteExtent(ino, Extent{ChunkID: "c0", ChunkOffset: 0, Length: 1024, FileOffset: 0}, n)
	checkInvariants(t, idx)

	newSize := int64(512)
	if err := idx.SetAttr(ino, nil, nil, nil, &newSize, nil, nil, n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	inode, _ := idx.GetInode(ino)
	if inode.Size != 512 {
		t.Errorf("size after truncate: got %d, want 512", inode.Size)
	}
}

func TestMkdir(t *testing.T) {
	idx := New(4096)
	n := now()

	dirIno, _ := idx.CreateInode(Directory, 0o755, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "subdir", dirIno, n)
	checkInvariants(t, idx)

	// Create a file inside subdir.
	fIno, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(dirIno, "file.txt", fIno, n)
	checkInvariants(t, idx)
}

func TestRmdir(t *testing.T) {
	idx := New(4096)
	n := now()

	dirIno, _ := idx.CreateInode(Directory, 0o755, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "empty", dirIno, n)
	checkInvariants(t, idx)

	if err := idx.RemoveDirEntry(RootIno, "empty", n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	if _, ok := idx.GetInode(dirIno); ok {
		t.Error("directory inode should have been freed")
	}
}

func TestRmdirNotEmpty(t *testing.T) {
	idx := New(4096)
	n := now()

	dirIno, _ := idx.CreateInode(Directory, 0o755, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "d", dirIno, n)
	fIno, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(dirIno, "f.txt", fIno, n)

	if err := idx.RemoveDirEntry(RootIno, "d", n); err == nil {
		t.Error("expected error removing non-empty directory")
	}
	checkInvariants(t, idx)
}

func TestRename(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "old.txt", ino, n)
	checkInvariants(t, idx)

	if err := idx.Rename(RootIno, "old.txt", RootIno, "new.txt", n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	if _, ok := idx.Lookup(RootIno, "old.txt"); ok {
		t.Error("old name should be gone after rename")
	}
	got, ok := idx.Lookup(RootIno, "new.txt")
	if !ok || got != ino {
		t.Errorf("new name: got %d, %v; want %d, true", got, ok, ino)
	}
}

func TestRenameReplacesDest(t *testing.T) {
	idx := New(4096)
	n := now()

	src, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	dst, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "src.txt", src, n)
	idx.AddDirEntry(RootIno, "dst.txt", dst, n)
	checkInvariants(t, idx)

	if err := idx.Rename(RootIno, "src.txt", RootIno, "dst.txt", n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	if _, ok := idx.GetInode(dst); ok {
		t.Error("replaced inode should be freed")
	}
	got, _ := idx.Lookup(RootIno, "dst.txt")
	if got != src {
		t.Errorf("dst.txt should point to src inode %d, got %d", src, got)
	}
}

func TestSymlink(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, _ := idx.CreateInode(Symlink, 0o777, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "link", ino, n)
	if err := idx.SetSymlink(ino, "/target/path", n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	target, ok := idx.GetSymlink(ino)
	if !ok || target != "/target/path" {
		t.Errorf("GetSymlink: got %q, %v", target, ok)
	}
	inode, _ := idx.GetInode(ino)
	if inode.Size != int64(len("/target/path")) {
		t.Errorf("symlink size: got %d, want %d", inode.Size, len("/target/path"))
	}
}

func TestXattrs(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "f.txt", ino, n)

	if err := idx.SetXattr(ino, "user.foo", "bar", n); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	attrs := idx.GetXattrs(ino)
	if attrs["user.foo"] != "bar" {
		t.Errorf("GetXattrs: got %q, want %q", attrs["user.foo"], "bar")
	}

	if err := idx.RemoveXattr(ino, "user.foo", n); err != nil {
		t.Fatal(err)
	}
	attrs = idx.GetXattrs(ino)
	if _, ok := attrs["user.foo"]; ok {
		t.Error("xattr should have been removed")
	}
}

func TestPersistRoundtrip(t *testing.T) {
	dir := t.TempDir()
	idx := New(4096)
	n := now()

	ino, _ := idx.CreateInode(Regular, 0o644, 1000, 1000, 0, n)
	idx.AddDirEntry(RootIno, "persisted.txt", ino, n)
	idx.WriteExtent(ino, Extent{ChunkID: "c0", ChunkOffset: 0, Length: 42, FileOffset: 0}, n)

	if err := Save(idx, dir); err != nil {
		t.Fatal(err)
	}
	if idx.IsDirty() {
		t.Error("index should not be dirty after Save")
	}

	idx2, err := Load(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx2)

	got, ok := idx2.Lookup(RootIno, "persisted.txt")
	if !ok || got != ino {
		t.Errorf("Lookup after restore: got %d, %v; want %d, true", got, ok, ino)
	}
	inode, _ := idx2.GetInode(ino)
	if inode.Size != 42 {
		t.Errorf("size after restore: got %d, want 42", inode.Size)
	}
}

func TestLoadNotExist(t *testing.T) {
	_, err := Load(t.TempDir(), 4096)
	if !os.IsNotExist(err) {
		t.Errorf("expected os.ErrNotExist, got %v", err)
	}
}

func TestUsedBytes(t *testing.T) {
	idx := New(4096)
	n := now()

	if idx.UsedBytes() != 0 {
		t.Errorf("UsedBytes on empty index: got %d, want 0", idx.UsedBytes())
	}

	ino1, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "a.txt", ino1, n)
	idx.WriteExtent(ino1, Extent{ChunkID: "c0", ChunkOffset: 0, Length: 1000, FileOffset: 0}, n)

	ino2, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "b.txt", ino2, n)
	idx.WriteExtent(ino2, Extent{ChunkID: "c0", ChunkOffset: 1000, Length: 500, FileOffset: 0}, n)

	// Directories should not count towards used bytes.
	dirIno, _ := idx.CreateInode(Directory, 0o755, 0, 0, 0, n)
	idx.AddDirEntry(RootIno, "dir", dirIno, n)

	if got := idx.UsedBytes(); got != 1500 {
		t.Errorf("UsedBytes: got %d, want 1500", got)
	}
}

func TestReaddir(t *testing.T) {
	idx := New(4096)
	n := now()

	names := []string{"a.txt", "b.txt", "c.txt"}
	for _, name := range names {
		ino, _ := idx.CreateInode(Regular, 0o644, 0, 0, 0, n)
		idx.AddDirEntry(RootIno, name, ino, n)
	}
	checkInvariants(t, idx)

	entries, err := idx.Readdir(RootIno)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool)
	for _, e := range entries {
		got[e.Name] = true
	}
	for _, name := range names {
		if !got[name] {
			t.Errorf("Readdir missing %q", name)
		}
	}
}

// --- Atomic make-node tests ---

func TestMakeFile(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, err := idx.MakeFile(RootIno, "hello.txt", 0o644, 1000, 1000, 0, n)
	if err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	inode, ok := idx.GetInode(ino)
	if !ok {
		t.Fatal("inode not found")
	}
	if inode.FileType != Regular {
		t.Errorf("FileType: got %v, want Regular", inode.FileType)
	}
	if inode.Nlink != 1 {
		t.Errorf("Nlink: got %d, want 1", inode.Nlink)
	}
	got, ok := idx.Lookup(RootIno, "hello.txt")
	if !ok || got != ino {
		t.Errorf("Lookup: got %d, %v; want %d, true", got, ok, ino)
	}

	// Duplicate name returns ErrExist.
	_, err = idx.MakeFile(RootIno, "hello.txt", 0o644, 0, 0, 0, n)
	if !errors.Is(err, ErrExist) {
		t.Errorf("duplicate MakeFile: got %v, want ErrExist", err)
	}
	checkInvariants(t, idx)
}

func TestMakeDir(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, err := idx.MakeDir(RootIno, "subdir", 0o755, 0, 0, n)
	if err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	inode, ok := idx.GetInode(ino)
	if !ok {
		t.Fatal("inode not found")
	}
	if inode.FileType != Directory {
		t.Errorf("FileType: got %v, want Directory", inode.FileType)
	}
	// nlink=2: one for parent entry, one for "." self-link.
	if inode.Nlink != 2 {
		t.Errorf("Nlink: got %d, want 2", inode.Nlink)
	}

	// Duplicate name returns ErrExist.
	_, err = idx.MakeDir(RootIno, "subdir", 0o755, 0, 0, n)
	if !errors.Is(err, ErrExist) {
		t.Errorf("duplicate MakeDir: got %v, want ErrExist", err)
	}
	checkInvariants(t, idx)
}

func TestMakeSymlink(t *testing.T) {
	idx := New(4096)
	n := now()

	ino, err := idx.MakeSymlink(RootIno, "link", "/target/path", 0, 0, n)
	if err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, idx)

	inode, ok := idx.GetInode(ino)
	if !ok {
		t.Fatal("inode not found")
	}
	if inode.FileType != Symlink {
		t.Errorf("FileType: got %v, want Symlink", inode.FileType)
	}
	if inode.Nlink != 1 {
		t.Errorf("Nlink: got %d, want 1", inode.Nlink)
	}
	if inode.Size != int64(len("/target/path")) {
		t.Errorf("Size: got %d, want %d", inode.Size, len("/target/path"))
	}
	target, ok := idx.GetSymlink(ino)
	if !ok || target != "/target/path" {
		t.Errorf("GetSymlink: got %q, %v", target, ok)
	}

	// Duplicate name returns ErrExist.
	_, err = idx.MakeSymlink(RootIno, "link", "/other", 0, 0, n)
	if !errors.Is(err, ErrExist) {
		t.Errorf("duplicate MakeSymlink: got %v, want ErrExist", err)
	}
	checkInvariants(t, idx)
}

// TestConcurrentMakeFile verifies that concurrent attempts to create the same
// file name leave no orphaned inodes in the index. Prior to the fix this test
// would fail the noOrphanedInodes invariant because the losing goroutines each
// succeeded at CreateInode (allocating an inode with nlink=0) before failing
// at AddDirEntry, leaving unreachable inodes in the map.
func TestConcurrentMakeFile(t *testing.T) {
	const workers = 50
	idx := New(4096)
	n := now()

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		successes int
		errs      []error
	)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, err := idx.MakeFile(RootIno, "contested.txt", 0o644, 0, 0, 0, n)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
			} else {
				errs = append(errs, err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Errorf("expected exactly 1 successful create, got %d", successes)
	}
	for _, err := range errs {
		if !errors.Is(err, ErrExist) {
			t.Errorf("expected ErrExist from losing goroutine, got: %v", err)
		}
	}
	checkInvariants(t, idx)
}
