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

// TestVersionSubcommand verifies the *contract* of the version subcommand
// rather than the *value* of any particular version:
//
//  1. `fusey version` exits 0 and prints `fusey <something>` — that is,
//     it prints *a* version, but the test does not pin which version. The
//     release version is a deployment-time concern; the binary's contract
//     is "I tell you my version".
//  2. The printed version observably changes when `-X main.Version=...`
//     ldflags are injected at build time. We verify this by comparing the
//     default build's printed version against the printed version from a
//     build with ldflags — they must differ, but we do not assert what
//     either is.
//  3. `fusey version`, `fusey --version`, and `fusey -v` all print the
//     *same* version for the same binary (cross-flag equivalence). They
//     are three spellings of one command.
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

	run := func(args ...string) string {
		t.Helper()
		c := exec.Command("go", "run", cmdFuseyDir)
		c.Args = append(c.Args, args...)
		out, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("%v\noutput: %s", err, out)
		}
		return strings.TrimRight(string(out), "\n")
	}

	// ldRun runs the package with an extra `-ldflags=...` arg. This is the
	// exact path goreleaser exercises at release time (see .goreleaser.yaml);
	// the inner `go run` invokes `go build` of a temporary main package, and
	// that build honors our ldflags.
	ldRun := func(ldflags string, args ...string) string {
		t.Helper()
		c := exec.Command("go", "run", "-ldflags="+ldflags, cmdFuseyDir)
		c.Args = append(c.Args, args...)
		out, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("%v\noutput: %s", err, out)
		}
		return strings.TrimRight(string(out), "\n")
	}

	// printedVersion extracts the version component from a `fusey <ver>`
	// output line, asserting that the prefix is present and the version is
	// non-empty. Returns the bare version string for equality comparison
	// across flag forms. We deliberately do NOT compare it against any
	// literal value — only against other `printedVersion` results.
	printedVersion := func(label, line string) string {
		t.Helper()
		const prefix = "fusey "
		if !strings.HasPrefix(line, prefix) {
			t.Fatalf("%s: output %q does not start with %q", label, line, prefix)
		}
		v := strings.TrimPrefix(line, prefix)
		if v == "" {
			t.Fatalf("%s: output %q has empty version component", label, line)
		}
		return v
	}

	// 1. Default build prints *some* version.
	defaultVer := printedVersion("default `version`", run("version"))

	// 2. ldflags injection observably changes the printed version.
	//
	// We don't pin the injected value (it's whatever the build system put
	// there at release time); we only require it to differ from the
	// no-ldflags default. An implementation that ignores ldflags would fail
	// this assertion.
	injectedVer := printedVersion(
		"ldflags-injected `version`",
		ldRun("-X main.Version="+defaultVer+"-OVERRIDDEN", "version"),
	)
	if injectedVer == defaultVer {
		t.Errorf(
			"ldflags-injected version did not differ from default (both = %q); "+
				"the `-X main.Version=...` mechanism is not wired up",
			defaultVer,
		)
	}

	// 3. Cross-flag equivalence. All three spellings of the version query
	// must agree for the same binary. The cross-form equality is the
	// contract; the actual version value is not.
	for _, flag := range []string{"version", "--version", "-v"} {
		got := printedVersion(
			"`" + flag + "`",
			ldRun("-X main.Version="+defaultVer+"-OVERRIDDEN", flag),
		)
		if got != injectedVer {
			t.Errorf(
				"`fusey %s` printed %q; expected %q (matching `fusey version`)",
				flag, got, injectedVer,
			)
		}
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
