// Command fusey manages a Fusey FUSE filesystem backed by S3-compatible storage.
//
// Subcommands:
//
//	fusey mount <mountpoint>    — start a background daemon; exits when mount is operational
//	fusey unmount <mountpoint>  — terminate the daemon serving the given mountpoint
//	fusey compact               — run one compaction cycle and exit
//
// All configuration is via FUSEY_* environment variables (see README).
// Each mount gets its own subdirectory under FUSEY_CACHE_DIR named by a random
// daemon ID: <FUSEY_CACHE_DIR>/<daemonID>/{index.cbor,chunks/,daemon.pid,fusey.log}.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/tofarr/fusey/internal/chunks"
	"github.com/tofarr/fusey/internal/compaction"
	"github.com/tofarr/fusey/internal/config"
	fusefs "github.com/tofarr/fusey/internal/fuse"
	"github.com/tofarr/fusey/internal/index"
	"github.com/tofarr/fusey/internal/journal"
)

// daemonInfo is written as JSON to <daemonDir>/daemon.pid so that
// `fusey unmount` can find and terminate the right background process.
type daemonInfo struct {
	PID        int    `json:"pid"`
	Mountpoint string `json:"mountpoint"`
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: fusey <mount|unmount|compact|journal-dump> [args]")
	}
	switch os.Args[1] {
	case "mount":
		// fusey mount <mountpoint> [--as-of=<RFC3339-timestamp>]
		mp, asOf, err := parseMountArgs(os.Args[2:])
		if err != nil {
			log.Fatal(err)
		}
		runMount(mp, asOf)
	case "daemon": // internal subcommand — launched by runMount; not for direct use
		// fusey daemon <daemonID> <mountpoint> [--as-of=<ts>]
		if len(os.Args) < 4 || len(os.Args) > 5 {
			log.Fatal("usage: fusey daemon <daemonID> <mountpoint> [--as-of=<ts>]")
		}
		var asOf time.Time
		if len(os.Args) == 5 {
			t, err := parseAsOf(os.Args[4])
			if err != nil {
				log.Fatal(err)
			}
			asOf = t
		}
		runDaemon(os.Args[2], os.Args[3], asOf)
	case "unmount":
		if len(os.Args) != 3 {
			log.Fatal("usage: fusey unmount <mountpoint>")
		}
		runUnmount(os.Args[2])
	case "compact":
		runCompact()
	case "journal-dump":
		// fusey journal-dump [--json]
		jsonOut := false
		for _, a := range os.Args[2:] {
			switch a {
			case "--json":
				jsonOut = true
			case "-h", "--help":
				fmt.Println("usage: fusey journal-dump [--json]")
				return
			default:
				log.Fatalf("unknown flag %q (try --json or --help)", a)
			}
		}
		runJournalDump(jsonOut)
	default:
		log.Fatalf("unknown subcommand %q; use 'mount', 'unmount', 'compact', or 'journal-dump'", os.Args[1])
	}
}

// parseMountArgs extracts <mountpoint> and an optional --as-of=<ts> from
// the args slice. The mountpoint is the first positional argument; the
// optional flag may appear anywhere afterwards.
func parseMountArgs(args []string) (string, time.Time, error) {
	if len(args) < 1 {
		return "", time.Time{}, fmt.Errorf("usage: fusey mount <mountpoint> [--as-of=<ts>]")
	}
	mp := args[0]
	var asOf time.Time
	for _, a := range args[1:] {
		const prefix = "--as-of="
		if !strings.HasPrefix(a, prefix) {
			return "", time.Time{}, fmt.Errorf("unknown flag %q (only --as-of=<ts> is supported)", a)
		}
		t, err := parseAsOf(a)
		if err != nil {
			return "", time.Time{}, err
		}
		asOf = t
	}
	return mp, asOf, nil
}

// parseAsOf accepts both "YYYY-MM-DDTHH:MM:SSZ" and the
// "YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ" forms. Sub-second precision is honoured
// when present.
func parseAsOf(s string) (time.Time, error) {
	s = strings.TrimPrefix(s, "--as-of=")
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid --as-of timestamp %q (expected RFC3339, e.g. 2026-01-15T10:00:00Z)", s)
}

// runMount spawns a background daemon for the given mountpoint and blocks
// until the daemon signals that the FUSE mount is established. It then exits
// with status 0, leaving the daemon running in the background.
//
// The daemon is launched as: fusey daemon <daemonID> <mountpoint>
// A pipe (fd 3 in the daemon) carries a single "ready\n" line back to the
// parent once gofs.Mount has returned. If the pipe closes without that signal
// the daemon failed; the parent exits non-zero and refers the user to the log.
func runMount(mountpoint string, asOf time.Time) {
	cfg := mustLoadConfig()

	daemonID := newDaemonID()
	daemonDir := filepath.Join(cfg.CacheDir, daemonID)
	if err := os.MkdirAll(daemonDir, 0755); err != nil {
		log.Fatalf("create daemon dir: %v", err)
	}

	// Capture the daemon's stderr (and therefore the stderr of any helper
	// go-fuse execs during the FUSE mount, such as fusermount3) into a sidecar
	// file. go-fuse v2.10.x inherits the parent's fd 2 for helper stderr and
	// only surfaces a cryptic "exit code <raw-wait-status>" message of its own;
	// the real kernel/syscall error from fusermount3 would otherwise be lost.
	mountLogPath := filepath.Join(daemonDir, "mount.log")
	mountLog, err := os.OpenFile(
		mountLogPath,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0644,
	)
	if err != nil {
		log.Fatalf("open mount log: %v", err)
	}
	defer mountLog.Close()

	r, w, err := os.Pipe()
	if err != nil {
		log.Fatalf("pipe: %v", err)
	}

	exe, err := os.Executable()
	if err != nil {
		log.Fatalf("executable: %v", err)
	}
	daemonArgs := []string{"daemon", daemonID, mountpoint}
	if !asOf.IsZero() {
		daemonArgs = append(daemonArgs, "--as-of="+asOf.UTC().Format(time.RFC3339Nano))
	}
	cmd := exec.Command(exe, daemonArgs...)
	cmd.Env = os.Environ()
	cmd.ExtraFiles = []*os.File{w} // becomes fd 3 in the daemon
	cmd.Stderr = mountLog            // fd 2 in the daemon — inherited by go-fuse helpers
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		log.Fatalf("start daemon: %v", err)
	}
	w.Close()

	buf, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(buf) != "ready\n" {
		log.Fatalf(
			"daemon failed to mount; check %s and %s",
			filepath.Join(daemonDir, "fusey.log"),
			mountLogPath,
		)
	}

	if asOf.IsZero() {
		fmt.Printf("fusey: mounted %s (daemon %s)\n", mountpoint, daemonID)
	} else {
		fmt.Printf("fusey: mounted %s (daemon %s, as-of=%s, read-only)\n",
			mountpoint, daemonID, asOf.UTC().Format(time.RFC3339Nano))
	}
}

// runDaemon is the long-running background process that serves FUSE requests.
// It is started by runMount and is not intended to be invoked directly.
//
// Protocol (see specs/mount.qnt):
//  1. Opens <daemonDir>/fusey.log and redirects all log output there.
//  2. Scopes cfg.CacheDir to <baseDir>/<daemonID> so all on-disk artefacts
//     (index.cbor, chunks/) are isolated to this mount instance.
//  3. Performs the FUSE mount. On success it writes a JSON PID file and sends
//     "ready\n" to the parent via fd 3 (the pipe write end); the parent exits.
//  4. Serves FUSE requests until SIGINT or SIGTERM, then unmounts, flushes the
//     index, removes the PID file, and exits.
//
// When asOf is non-zero, the daemon is in read-only "as-of" mode: it loads
// the journal base, replays journal entries with ts <= asOf, and mounts the
// resulting state read-only. As-of mounts cannot be unmounted via `fusey
// unmount` because they don't write a daemon.pid file; SIGTERM the
// underlying `fusey mount` process instead.
func runDaemon(daemonID, mountpoint string, asOf time.Time) {
	cfg := mustLoadConfig()

	daemonDir := filepath.Join(cfg.CacheDir, daemonID)
	if err := os.MkdirAll(daemonDir, 0755); err != nil {
		log.Fatalf("daemon dir: %v", err)
	}

	logFile, err := os.OpenFile(
		filepath.Join(daemonDir, "fusey.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		log.Fatalf("open log: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// fd 3 is the write end of the ready pipe; closing it signals failure.
	readyPipe := os.NewFile(3, "ready-pipe")

	cfg.CacheDir = daemonDir // isolate all on-disk artefacts to this daemon

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objStore, cs := mustBuildStore(ctx, cfg)

	// As-of mode loads the base, loads the journal shards, and replays.
	// Live mode loads the index (from disk or remote) and uses it directly.
	readOnly := !asOf.IsZero()
	var idx *index.Index
	if readOnly {
		idx = mustLoadAsOfIndex(ctx, cfg, objStore, asOf)
	} else {
		idx = loadIndex(ctx, cfg, objStore)
	}

	// Wire the journal (no-op when cfg.JournalEnabled is false). The
	// JournaledIndex wraps the underlying index; the FUSE layer receives
	// the wrapper and so transparently records every state-mutating op.
	// In as-of mode the journal is irrelevant (we're read-only) so we
	// skip wrapping and pass the bare index.
	var fsIdx fusefs.FSIndex = idx
	var j *journal.Journal
	if cfg.JournalEnabled && !readOnly {
		j = journal.New(true)
		// On startup, restore any unflushed journal entries from previous
		// daemons that crashed before persisting. The in-memory buffer is
		// empty at this point so this is the only state to load.
		loaded, err := journal.LoadAll(ctx, objStore)
		if err != nil {
			log.Fatalf("load journal: %v", err)
		}
		for _, e := range loaded {
			j.Append(e.Op, e.Ts) // nextSeq would need to be set, see note below
		}
		// If we loaded any entries, our nextSeq must advance past them.
		// Append assigns from internal nextSeq starting at 1, so we need
		// to re-derive the nextSeq from the max loaded seq.
		if len(loaded) > 0 {
			if err := j.SetNextSeqAfter(loaded); err != nil {
				log.Fatalf("set journal nextSeq: %v", err)
			}
		}
		fsIdx = journal.Wrap(idx, j)
	}

	// In live mode, build the persist function (which now also flushes
	// the journal). In as-of mode there is nothing to persist.
	var persistFn func(context.Context) error
	if !readOnly {
		persistFn = buildPersistFn(cfg, idx, cs, objStore, j)
	}

	f := fusefs.New(fsIdx, cs, cfg.MaxFSSize, cfg.CacheDir, readOnly)
	mountOpts := fuse.MountOptions{
		FsName:      "fusey",
		AllowOther:  false,
		DirectMount: true,
	}
	if readOnly {
		// go-fuse v2.10.1 has no dedicated ReadOnly field on MountOptions;
		// pass the standard "ro" mount option. The kernel will then
		// reject all write operations with EROFS, which is exactly the
		// behaviour we want for an as-of mount.
		mountOpts.Options = append(mountOpts.Options, "ro")
	}
	server, err := gofs.Mount(mountpoint, f.Root(), &gofs.Options{MountOptions: mountOpts})
	if err != nil {
		readyPipe.Close() // EOF on parent's read end signals mount failure
		log.Fatalf("mount: %v", err)
	}
	if readOnly {
		log.Printf("mounted at %s (daemon %s, as-of=%s, read-only)",
			mountpoint, daemonID, asOf.UTC().Format(time.RFC3339Nano))
	} else {
		log.Printf("mounted at %s (daemon %s)", mountpoint, daemonID)
	}

	// Write PID file for unmount lookup before signalling the parent, so that
	// `fusey unmount` cannot race against a daemon that has not yet written it.
	// In as-of mode we skip this so `fusey unmount` does not see the mount
	// (as-of mounts are managed by killing the parent `fusey mount` process).
	if !readOnly {
		pidPath := filepath.Join(daemonDir, "daemon.pid")
		pidData, _ := json.Marshal(daemonInfo{PID: os.Getpid(), Mountpoint: mountpoint})
		_ = os.WriteFile(pidPath, append(pidData, '\n'), 0644)
	}

	// Signal the parent: filesystem is accessible; parent will exit 0.
	_, _ = fmt.Fprint(readyPipe, "ready\n")
	readyPipe.Close()

	// Periodic index persistence — exits cleanly when ctx is cancelled.
	// As-of mode has nothing to persist (and must not: writes would corrupt
	// the historical state).
	if !readOnly {
		go func() {
			ticker := time.NewTicker(cfg.PersistInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if idx.IsDirty() || idx.IsRemoteDirty() {
						if err := persistFn(ctx); err != nil {
							log.Printf("persist index: %v", err)
						}
					}
				}
			}
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel() // stop the periodic persist goroutine

	log.Println("unmounting...")
	if err := server.Unmount(); err != nil {
		log.Printf("unmount: %v", err)
	}
	// Use a fresh context for the final flush: the daemon's context was just
	// cancelled to stop the background goroutine, and using it here would cause
	// all S3/broker calls to fail immediately before any data is written.
	if persistFn != nil {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer flushCancel()
		if err := persistFn(flushCtx); err != nil {
			log.Printf("final persist: %v", err)
		}
	}
	if !readOnly {
		_ = os.Remove(filepath.Join(daemonDir, "daemon.pid"))
	}
	log.Println("done")
}

// runUnmount scans <FUSEY_CACHE_DIR>/*/daemon.pid to find the daemon serving
// mountpoint and sends it SIGTERM. The daemon handles the signal by unmounting,
// flushing the index, and exiting.
func runUnmount(mountpoint string) {
	cfg := mustLoadConfig()

	abs, err := filepath.Abs(mountpoint)
	if err != nil {
		log.Fatalf("resolve mountpoint: %v", err)
	}

	entries, err := os.ReadDir(cfg.CacheDir)
	if err != nil {
		log.Fatalf("read cache dir %s: %v", cfg.CacheDir, err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pidPath := filepath.Join(cfg.CacheDir, e.Name(), "daemon.pid")
		data, err := os.ReadFile(pidPath)
		if err != nil {
			continue
		}
		var info daemonInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}
		infoAbs, _ := filepath.Abs(info.Mountpoint)
		if infoAbs != abs {
			continue
		}
		proc, err := os.FindProcess(info.PID)
		if err != nil {
			log.Fatalf("find process %d: %v", info.PID, err)
		}
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			log.Fatalf("signal daemon %d: %v", info.PID, err)
		}
		fmt.Printf("fusey: sent SIGTERM to daemon %s (pid %d)\n", e.Name(), info.PID)
		return
	}

	log.Fatalf("no active daemon found for mountpoint %s", mountpoint)
}

// runCompact loads the index from S3, runs one compaction cycle, persists the
// updated index, and exits. Intended to be called from a Kubernetes CronJob.
//
// When the journal is enabled, runCompact first deletes the journal shards
// (per the design decision: at the START of the cycle, before any chunk
// remap), then runs the compaction, then writes a fresh base snapshot so
// subsequent as-of mounts can replay against the post-compact state.
func runCompact() {
	cfg := mustLoadConfig()
	ctx := context.Background()

	objStore, cs := mustBuildStore(ctx, cfg)
	idx := loadIndex(ctx, cfg, objStore)

	if cfg.JournalEnabled {
		log.Println("clearing journal (start-of-compact cycle)")
		if err := journal.Clear(ctx, objStore, cfg.CacheDir); err != nil {
			log.Fatalf("clear journal: %v", err)
		}
	}

	persistFn := buildPersistFn(cfg, idx, cs, objStore, nil)
	comp := compaction.New(idx, cs, persistFn, cfg.CompactionThreshold, cfg.ChunkSize)

	log.Println("starting compaction cycle")
	if err := comp.Compact(ctx); err != nil {
		log.Fatalf("compact: %v", err)
	}

	// After a successful compact, refresh the base snapshot so future
	// as-of mounts can replay against the post-compact state. We do this
	// even when the journal is disabled so the file is created on first
	// run, but it's only meaningful when the journal is enabled.
	if cfg.JournalEnabled {
		log.Println("writing fresh base snapshot for as-of replay")
		if err := journal.SaveBase(idx, cfg.CacheDir, objStore); err != nil {
			log.Fatalf("save base: %v", err)
		}
	}
	log.Println("compaction complete")
}

// --- shared helpers ---

func mustLoadConfig() *config.Config {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if cfg.BrokerURL == "" && cfg.Bucket == "" {
		log.Fatal("either FUSEY_BROKER_URL or FUSEY_BUCKET is required")
	}
	return cfg
}

// mustBuildStore constructs the appropriate ObjectStore based on config.
// When FUSEY_BROKER_URL is set, a BrokerStore is used; otherwise an S3Store.
func mustBuildStore(ctx context.Context, cfg *config.Config) (chunks.ObjectStore, *chunks.ChunkStore) {
	var (
		objStore chunks.ObjectStore
		cs       *chunks.ChunkStore
	)
	if cfg.BrokerURL != "" {
		log.Printf("using broker store: %s", cfg.BrokerURL)
		bs := chunks.NewBrokerStore(cfg.BrokerURL, cfg.BrokerAuthHeader, cfg.BrokerAuthValue)
		objStore = bs
	} else {
		log.Printf("using S3 store: bucket=%s endpoint=%s", cfg.Bucket, cfg.Endpoint)
		s3store, err := chunks.NewS3Store(
			ctx,
			cfg.Bucket, cfg.Endpoint, cfg.Region,
			cfg.AccessKey, cfg.SecretKey,
			cfg.Prefix,
			cfg.ForcePathStyle,
		)
		if err != nil {
			log.Fatalf("S3 store: %v", err)
		}
		objStore = s3store
	}
	cs = chunks.NewChunkStore(objStore, cfg.ChunkSize)
	if err := cs.RecoverNextSeq(ctx); err != nil {
		log.Fatalf("recover chunk sequence: %v", err)
	}
	if err := cs.SetCacheDir(cfg.CacheDir); err != nil {
		log.Fatalf("chunk cache: %v", err)
	}
	return objStore, cs
}

// buildPersistFn returns a function that persists the index locally and then
// to the remote object store. The returned function accepts a context so that
// callers can supply different contexts for background periodic flushes versus
// the final shutdown flush (which must use a fresh, non-cancelled context).
//
// Ordering rationale:
//  1. index.Save (local disk) — runs when the in-memory index has diverged
//     from disk. The broker's availability must not gate local durability;
//     the pod can recover from its local cache even if the remote write fails.
//  2. cs.FlushActive (remote chunk) — runs before the remote index write so
//     the remote store is always self-consistent: every extent the persisted
//     index references exists as a chunk object in the store. FlushActive is a
//     no-op when the active buffer has not been modified since the last flush.
//  3. journal.Flush (remote journal shards) — only when the journal is
//     enabled and has unflushed entries. New shards are appended, never
//     overwriting existing ones. This is what enables as-of replay.
//  4. store.PutRaw (remote index) — written last, and only when the index has
//     structural mutations since the last remote write (idx.IsRemoteDirty()).
//     Atime-only updates from reads do not set the remote-dirty flag, so pure
//     read workloads generate no presigned-URL round-trips.
func buildPersistFn(
	cfg *config.Config, idx *index.Index, cs *chunks.ChunkStore,
	store chunks.ObjectStore, j *journal.Journal,
) func(context.Context) error {
	return func(ctx context.Context) error {
		if idx.IsDirty() {
			if err := index.Save(idx, cfg.CacheDir); err != nil {
				return err
			}
		}
		if err := cs.FlushActive(ctx); err != nil {
			return err
		}
		if j != nil && j.IsDirty() {
			if _, _, err := journal.Flush(ctx, j, store); err != nil {
				return err
			}
		}
		if !idx.IsRemoteDirty() {
			return nil
		}
		data, err := index.Marshal(idx)
		if err != nil {
			return err
		}
		if err := store.PutRaw(ctx, store.IndexKey(), data); err != nil {
			return err
		}
		idx.MarkRemoteClean()
		return nil
	}
}

// loadIndex tries to restore the index from (in order):
//  1. Local disk cache — fastest path, used on warm restarts of the same pod.
//  2. Object store (S3 or broker) — when the local cache is absent.
//  3. Empty index — genuinely fresh filesystem (first ever mount).
func loadIndex(ctx context.Context, cfg *config.Config, store chunks.ObjectStore) *index.Index {
	idx, err := index.Load(cfg.CacheDir, cfg.BlockSize)
	if err == nil {
		log.Printf("loaded index from local cache %s", cfg.CacheDir)
		return idx
	}
	if !os.IsNotExist(err) {
		log.Fatalf("load index from disk: %v", err)
	}

	data, err := store.GetRaw(ctx, store.IndexKey())
	if err == nil {
		idx, err = index.Unmarshal(data, cfg.BlockSize)
		if err != nil {
			log.Fatalf("parse index from object store: %v", err)
		}
		log.Printf("loaded index from object store (%s)", store.IndexKey())
		if saveErr := index.Save(idx, cfg.CacheDir); saveErr != nil {
			log.Printf("warn: could not cache index locally: %v", saveErr)
		}
		return idx
	}
	if !errors.Is(err, chunks.ErrNotFound) {
		log.Fatalf("load index from object store: %v", err)
	}

	log.Printf("no existing index found; starting fresh filesystem")
	return index.New(cfg.BlockSize)
}

// newDaemonID returns a random 16-character hex string that uniquely identifies
// a daemon instance and names its subdirectory under FUSEY_CACHE_DIR.
func newDaemonID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("generate daemon ID: %v", err)
	}
	return hex.EncodeToString(b)
}

// mustLoadAsOfIndex loads the journal base snapshot and replays every
// journal entry with ts <= asOf to reconstruct the historical state. The
// returned index is suitable for read-only mounting.
//
// Errors that indicate "the journal is not available" (e.g. ErrNotFound
// from LoadBase, no shards from LoadAll) are returned as a user-facing
// message that explains how to enable the journal.
func mustLoadAsOfIndex(ctx context.Context, cfg *config.Config, store chunks.ObjectStore, asOf time.Time) *index.Index {
	base, err := journal.LoadBase(ctx, cfg.CacheDir, store)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, chunks.ErrNotFound) {
			log.Fatalf("as-of mount requires the journal; no base snapshot found. " +
				"Enable the journal by setting FUSEY_JOURNAL_ENABLED=true and run " +
				"`fusey mount` at least once to capture a base.")
		}
		log.Fatalf("load base for as-of mount: %v", err)
	}
	entries, err := journal.LoadAll(ctx, store)
	if err != nil {
		log.Fatalf("load journal entries for as-of mount: %v", err)
	}
	if len(entries) == 0 {
		log.Fatalf("as-of mount requires the journal; no journal shards found. " +
			"(Has the journal been wiped by `fusey compact`?)")
	}
	targetTs := asOf.UnixNano()
	replayed, err := journal.ReplayUpTo(base, entries, targetTs)
	if err != nil {
		log.Fatalf("replay journal for as-of mount: %v", err)
	}
	log.Printf("as-of mount: %d journal entries applied (target ts=%s, oldest ts=%s, newest ts=%s)",
		countUpTo(entries, targetTs),
		asOf.UTC().Format(time.RFC3339Nano),
		time.Unix(0, entries[0].Ts).UTC().Format(time.RFC3339Nano),
		time.Unix(0, entries[len(entries)-1].Ts).UTC().Format(time.RFC3339Nano),
	)
	return replayed
}

// countUpTo returns the number of entries with ts <= targetTs.
func countUpTo(es []journal.Entry, targetTs int64) int {
	n := 0
	for _, e := range es {
		if e.Ts <= targetTs {
			n++
		} else {
			break
		}
	}
	return n
}

// runJournalDump reads all journal shards from the backing store and
// writes them in the chosen format (human-readable by default, JSON with
// --json). Errors from the store are fatal.
func runJournalDump(jsonOut bool) {
	cfg := mustLoadConfig()
	ctx := context.Background()

	objStore, _ := mustBuildStore(ctx, cfg)

	entries, err := journal.LoadAll(ctx, objStore)
	if err != nil {
		log.Fatalf("load journal: %v", err)
	}
	if len(entries) == 0 {
		fmt.Fprintln(os.Stderr, "no journal shards found (journal may be disabled or have been wiped by a recent compact)")
		return
	}
	if jsonOut {
		if err := journal.DumpJSON(os.Stdout, entries); err != nil {
			log.Fatalf("dump json: %v", err)
		}
	} else {
		if err := journal.Dump(os.Stdout, entries); err != nil {
			log.Fatalf("dump: %v", err)
		}
	}
}
