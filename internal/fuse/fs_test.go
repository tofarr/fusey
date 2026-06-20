//go:build linux

package fuse

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/tofarr/fusey/internal/chunks"
	"github.com/tofarr/fusey/internal/index"
)

// mountFS mounts a fresh Fusey filesystem to a temp directory and returns
// the mount path and a cleanup function.
func mountFS(t *testing.T) (mnt string, cleanup func()) {
	t.Helper()
	local, err := chunks.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	idx := index.New(4096)
	cs := chunks.NewChunkStore(local, 64*1024*1024)
	f := New(idx, cs, 10*1024*1024*1024) // 10 GiB for tests

	mnt = t.TempDir()
	server, err := gofs.Mount(mnt, f.Root(), &gofs.Options{
		MountOptions: fuse.MountOptions{
			Debug:      false,
			FsName:     "fusey-test",
			AllowOther: false,
		},
	})
	if err != nil {
		t.Skipf("FUSE mount failed (no /dev/fuse?): %v", err)
	}
	return mnt, func() {
		server.Unmount()
	}
}

// --- Tests ---

func TestMountAndStatRoot(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	fi, err := os.Stat(mnt)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Error("root should be a directory")
	}
}

func TestCreateAndRead(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	path := filepath.Join(mnt, "hello.txt")
	content := []byte("hello, fusey!")

	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("ReadFile: got %q, want %q", got, content)
	}
}

func TestStatFile(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	path := filepath.Join(mnt, "stat.txt")
	content := []byte("stat test")
	os.WriteFile(path, content, 0o644)

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != int64(len(content)) {
		t.Errorf("Size: got %d, want %d", fi.Size(), len(content))
	}
	if fi.Mode()&0o644 == 0 {
		t.Errorf("Mode: got %o, want at least 644", fi.Mode())
	}
}

func TestWriteOverwrite(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	path := filepath.Join(mnt, "overwrite.txt")
	os.WriteFile(path, []byte("initial content here"), 0o644)
	os.WriteFile(path, []byte("replaced"), 0o644)

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("replaced")) {
		t.Errorf("after overwrite: got %q", got)
	}
}

func TestMkdirAndReaddir(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	dir := filepath.Join(mnt, "subdir")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644)
	os.WriteFile(filepath.Join(dir, "b.txt"), []byte("b"), 0o644)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name()] = true
	}
	if !names["a.txt"] || !names["b.txt"] {
		t.Errorf("ReadDir: got %v, want a.txt and b.txt", names)
	}
}

func TestUnlink(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	path := filepath.Join(mnt, "todelete.txt")
	os.WriteFile(path, []byte("bye"), 0o644)

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should not exist after unlink")
	}
}

func TestRmdir(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	dir := filepath.Join(mnt, "emptydir")
	os.Mkdir(dir, 0o755)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Error("directory should not exist after rmdir")
	}
}

func TestRmdirNotEmpty(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	dir := filepath.Join(mnt, "nonempty")
	os.Mkdir(dir, 0o755)
	os.WriteFile(filepath.Join(dir, "f.txt"), []byte("x"), 0o644)

	err := os.Remove(dir)
	if err == nil {
		t.Error("expected error removing non-empty directory")
	}
}

func TestRename(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	src := filepath.Join(mnt, "old.txt")
	dst := filepath.Join(mnt, "new.txt")
	os.WriteFile(src, []byte("renamed"), 0o644)

	if err := os.Rename(src, dst); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Error("source should not exist after rename")
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("renamed")) {
		t.Errorf("renamed file content: got %q", got)
	}
}

func TestHardLink(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	src := filepath.Join(mnt, "original.txt")
	lnk := filepath.Join(mnt, "link.txt")
	os.WriteFile(src, []byte("shared content"), 0o644)

	if err := os.Link(src, lnk); err != nil {
		t.Fatal(err)
	}
	// Both names should read the same content.
	got, _ := os.ReadFile(lnk)
	if !bytes.Equal(got, []byte("shared content")) {
		t.Errorf("hard link content: got %q", got)
	}
	// nlink should be 2.
	fi, _ := os.Stat(src)
	if fi.Sys().(*syscall.Stat_t).Nlink != 2 {
		t.Errorf("nlink: got %d, want 2", fi.Sys().(*syscall.Stat_t).Nlink)
	}
}

func TestSymlink(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	target := filepath.Join(mnt, "target.txt")
	link := filepath.Join(mnt, "link")
	os.WriteFile(target, []byte("target content"), 0o644)

	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	got, err := os.Readlink(link)
	if err != nil {
		t.Fatal(err)
	}
	if got != target {
		t.Errorf("Readlink: got %q, want %q", got, target)
	}
}

func TestChmod(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	path := filepath.Join(mnt, "mode.txt")
	os.WriteFile(path, []byte("x"), 0o644)
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(path)
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("Mode after chmod: got %o, want 600", fi.Mode().Perm())
	}
}

func TestTruncate(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	path := filepath.Join(mnt, "trunc.txt")
	os.WriteFile(path, []byte("hello world"), 0o644)
	if err := os.Truncate(path, 5); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, []byte("hello")) {
		t.Errorf("after truncate: got %q, want %q", got, "hello")
	}
}

func TestLargeFile(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	path := filepath.Join(mnt, "large.bin")
	// Write 2 MiB of data across multiple chunks.
	data := bytes.Repeat([]byte{0xAB}, 2*1024*1024)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("large file: content mismatch (len got=%d, want=%d)", len(got), len(data))
	}
}

func TestXattrs(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	path := filepath.Join(mnt, "xattr.txt")
	os.WriteFile(path, []byte("x"), 0o644)

	if err := syscall.Setxattr(path, "user.foo", []byte("bar"), 0); err != nil {
		t.Skipf("setxattr not supported: %v", err)
	}
	buf := make([]byte, 64)
	n, err := syscall.Getxattr(path, "user.foo", buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "bar" {
		t.Errorf("getxattr: got %q, want %q", buf[:n], "bar")
	}
}

func TestStatfs(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	var st syscall.Statfs_t
	if err := syscall.Statfs(mnt, &st); err != nil {
		t.Fatal(err)
	}
	// Total blocks must be > 0 (derived from FUSEY_MAX_SIZE / block_size).
	if st.Blocks == 0 {
		t.Error("Statfs Blocks should be > 0")
	}
	// Free must not exceed total.
	if st.Bfree > st.Blocks {
		t.Errorf("Statfs Bfree (%d) > Blocks (%d)", st.Bfree, st.Blocks)
	}
	// Write a file and verify free space decreases.
	blocksBefore := st.Bfree
	os.WriteFile(filepath.Join(mnt, "space.bin"), bytes.Repeat([]byte{0}, 4096*10), 0o644)
	var st2 syscall.Statfs_t
	syscall.Statfs(mnt, &st2)
	if st2.Bfree >= blocksBefore {
		t.Errorf("Bfree did not decrease after write: before=%d after=%d", blocksBefore, st2.Bfree)
	}
}

func TestTimestamps(t *testing.T) {
	mnt, cleanup := mountFS(t)
	defer cleanup()

	path := filepath.Join(mnt, "ts.txt")
	before := time.Now().Truncate(time.Second)
	os.WriteFile(path, []byte("ts"), 0o644)
	after := time.Now().Add(time.Second)

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	mtime := fi.ModTime()
	if mtime.Before(before) || mtime.After(after) {
		t.Errorf("mtime %v outside expected range [%v, %v]", mtime, before, after)
	}
}
