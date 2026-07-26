//go:build linux

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestRunMountCapturesMountLog verifies that when the daemon fails to mount,
// runMount has created <daemonDir>/mount.log before spawning the daemon and
// the parent's failure message points at both fusey.log and mount.log.
//
// The test invokes `go run` on this package's source main() — NOT the test
// binary. Re-execing the test binary via os.Executable() does not work: a
// `go test` binary has its main() replaced by testing.Main(), so it would
// run all tests (including this one) in the subprocess, causing infinite
// recursion. Using `go run` on the source directory gives us a real fusey
// binary whose main() dispatches to runMount based on os.Args[1].
//
// Skips when /dev/fuse is not available or when `go` is not in PATH,
// matching the patterns used in internal/fuse/fs_test.go and the CI runner.
func TestRunMountCapturesMountLog(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("/dev/fuse not available: %v", err)
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skipf("go not in PATH: %v", err)
	}

	cacheDir := t.TempDir()

	// The test file lives in cmd/fusey/, which is the package we want
	// `go run` to compile. runtime.Caller(0) gives us this path reliably
	// regardless of where the test is invoked from.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	cmdFuseyDir := filepath.Dir(thisFile)

	// A mountpoint under our temp dir that does not exist (and is not created
	// in any setup step). gofs.Mount will fail on it — either in mountDirect
	// (syscall.Stat returns ENOENT) or in the callFusermount fallback.
	badMount := filepath.Join(cacheDir, "does-not-exist")

	// mustLoadConfig requires either FUSEY_BROKER_URL or FUSEY_BUCKET. We
	// point the broker at an unreachable port so the daemon subprocess fails
	// fast (connection refused on the first request; ~1.5s with broker's
	// 4-attempt exponential backoff) instead of hanging on a real-but-slow
	// backend. The point of this test is the parent's failure path, not the
	// daemon's storage round-trip.
	const unreachableBroker = "http://127.0.0.1:1"

	// Run the source, not the test binary. We invoke go run with the absolute
	// package directory so this works regardless of working directory.
	cmd := exec.Command("go", "run", cmdFuseyDir, "mount", badMount)
	cmd.Env = append(os.Environ(),
		"FUSEY_CACHE_DIR="+cacheDir,
		"FUSEY_BROKER_URL="+unreachableBroker,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected fusey mount to fail; output:\n%s", out)
	}

	outStr := string(out)

	// The parent's failure message must mention both log files so the user
	// knows where to look. This is the user-facing half of the fix.
	if !strings.Contains(outStr, "mount.log") {
		t.Errorf("expected failure message to mention mount.log; got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "fusey.log") {
		t.Errorf("expected failure message to mention fusey.log; got:\n%s", outStr)
	}

	// runMount must have created <daemonDir>/mount.log inside the cache dir.
	// The daemon ID is random, so we look for any daemonDir/mount.log pair.
	mountLog, err := findMountLog(cacheDir)
	if err != nil {
		t.Fatalf("mount.log not found under %s: %v; output:\n%s", cacheDir, err, out)
	}

	// mount.log should exist and be a regular file. We don't assert non-empty
	// because the daemon's stderr could legitimately be empty in some failure
	// modes (e.g. gofs.Mount returns before any helper writes to fd 2).
	info, err := os.Stat(mountLog)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Errorf("mount.log is not a regular file: mode=%v", info.Mode())
	}
}

// TestVersionSubcommand verifies that:
//
//  1. `fusey version` exits 0 and prints `fusey dev` when the binary is built
//     from source (no ldflags).
//  2. `fusey version` reflects an injected `-X main.Version=...` ldflags
//     value — the same mechanism goreleaser uses at release time.
//  3. `fusey --version` and `fusey -v` (top-level flags) are accepted
//     shorthands for the subcommand and produce identical output.
//
// Like TestRunMountCapturesMountLog we run the real binary via `go run` on
// the package source rather than re-execing the test binary (whose main()
// is testing.Main(), causing infinite recursion). Unlike that test we do
// NOT skip on missing /dev/fuse — `version` is a pure stdout operation. We
// do skip when `go` is not in PATH so the test still runs on minimal CI
// runners that lack it.
func TestVersionSubcommand(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skipf("go not in PATH: %v", err)
	}

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	cmdFuseyDir := filepath.Dir(thisFile)

	run := func(args ...string) (string, error) {
		c := exec.Command("go", "run", cmdFuseyDir)
		c.Args = append(c.Args, args...)
		out, err := c.CombinedOutput()
		return strings.TrimRight(string(out), "\n"), err
	}

	// 1. Default (no ldflags) — must report `fusey dev`.
	if got, err := run("version"); err != nil {
		t.Fatalf("`fusey version` (default): %v\noutput: %s", err, got)
	} else if got != "fusey dev" {
		t.Errorf("`fusey version` (default): got %q, want %q", got, "fusey dev")
	}

	// 2. ldflags-injected — must report the injected value. `go run -ldflags=...`
	// propagates ldflags to the inner `go build` of the generated temporary
	// main package; this is the exact path goreleaser exercises at release
	// time (see .goreleaser.yaml).
	ldRun := func(ldflags string, args ...string) (string, error) {
		c := exec.Command("go", "run", "-ldflags="+ldflags, cmdFuseyDir)
		c.Args = append(c.Args, args...)
		out, err := c.CombinedOutput()
		return strings.TrimRight(string(out), "\n"), err
	}
	if got, err := ldRun("-X main.Version=vTEST-from-ldflags", "version"); err != nil {
		t.Fatalf("`fusey version` (ldflags): %v\noutput: %s", err, got)
	} else if got != "fusey vTEST-from-ldflags" {
		t.Errorf("`fusey version` (ldflags): got %q, want %q", got, "fusey vTEST-from-ldflags")
	}

	// 3. `fusey --version` shorthand — same output, no subcommand required.
	if got, err := ldRun("-X main.Version=vTEST-from-ldflags", "--version"); err != nil {
		t.Fatalf("`fusey --version`: %v\noutput: %s", err, got)
	} else if got != "fusey vTEST-from-ldflags" {
		t.Errorf("`fusey --version`: got %q, want %q", got, "fusey vTEST-from-ldflags")
	}

	// 4. `fusey -v` shorthand — same output.
	if got, err := ldRun("-X main.Version=vTEST-from-ldflags", "-v"); err != nil {
		t.Fatalf("`fusey -v`: %v\noutput: %s", err, got)
	} else if got != "fusey vTEST-from-ldflags" {
		t.Errorf("`fusey -v`: got %q, want %q", got, "fusey vTEST-from-ldflags")
	}
}

// findMountLog searches cacheDir for <some-daemon-dir>/mount.log. The daemon ID
// is generated by newDaemonID() so we don't know it up front; we just need to
// find the file that runMount left behind.
func findMountLog(cacheDir string) (string, error) {
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		candidate := filepath.Join(cacheDir, e.Name(), "mount.log")
		if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
			return candidate, nil
		}
	}
	return "", &os.PathError{Op: "find", Path: cacheDir, Err: os.ErrNotExist}
}
