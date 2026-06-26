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
	"syscall"
	"time"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/tofarr/fusey/internal/chunks"
	"github.com/tofarr/fusey/internal/compaction"
	"github.com/tofarr/fusey/internal/config"
	fusefs "github.com/tofarr/fusey/internal/fuse"
	"github.com/tofarr/fusey/internal/index"
)

// daemonInfo is written as JSON to <daemonDir>/daemon.pid so that
// `fusey unmount` can find and terminate the right background process.
type daemonInfo struct {
	PID        int    `json:"pid"`
	Mountpoint string `json:"mountpoint"`
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: fusey <mount|unmount|compact> [args]")
	}
	switch os.Args[1] {
	case "mount":
		if len(os.Args) != 3 {
			log.Fatal("usage: fusey mount <mountpoint>")
		}
		runMount(os.Args[2])
	case "daemon": // internal subcommand — launched by runMount; not for direct use
		if len(os.Args) != 4 {
			log.Fatal("usage: fusey daemon <daemonID> <mountpoint>")
		}
		runDaemon(os.Args[2], os.Args[3])
	case "unmount":
		if len(os.Args) != 3 {
			log.Fatal("usage: fusey unmount <mountpoint>")
		}
		runUnmount(os.Args[2])
	case "compact":
		runCompact()
	default:
		log.Fatalf("unknown subcommand %q; use 'mount', 'unmount', or 'compact'", os.Args[1])
	}
}

// runMount spawns a background daemon for the given mountpoint and blocks
// until the daemon signals that the FUSE mount is established. It then exits
// with status 0, leaving the daemon running in the background.
//
// The daemon is launched as: fusey daemon <daemonID> <mountpoint>
// A pipe (fd 3 in the daemon) carries a single "ready\n" line back to the
// parent once gofs.Mount has returned. If the pipe closes without that signal
// the daemon failed; the parent exits non-zero and refers the user to the log.
func runMount(mountpoint string) {
	cfg := mustLoadConfig()

	daemonID := newDaemonID()
	daemonDir := filepath.Join(cfg.CacheDir, daemonID)
	if err := os.MkdirAll(daemonDir, 0755); err != nil {
		log.Fatalf("create daemon dir: %v", err)
	}

	r, w, err := os.Pipe()
	if err != nil {
		log.Fatalf("pipe: %v", err)
	}

	exe, err := os.Executable()
	if err != nil {
		log.Fatalf("executable: %v", err)
	}
	cmd := exec.Command(exe, "daemon", daemonID, mountpoint)
	cmd.Env = os.Environ()
	cmd.ExtraFiles = []*os.File{w} // becomes fd 3 in the daemon
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		log.Fatalf("start daemon: %v", err)
	}
	w.Close()

	buf, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(buf) != "ready\n" {
		log.Fatalf("daemon failed to mount; check %s", filepath.Join(daemonDir, "fusey.log"))
	}

	fmt.Printf("fusey: mounted %s (daemon %s)\n", mountpoint, daemonID)
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
func runDaemon(daemonID, mountpoint string) {
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
	idx := loadIndex(ctx, cfg, objStore)
	persistFn := buildPersistFn(cfg, idx, cs, objStore)

	f := fusefs.New(idx, cs, cfg.MaxFSSize, cfg.CacheDir)
	server, err := gofs.Mount(mountpoint, f.Root(), &gofs.Options{
		MountOptions: fuse.MountOptions{
			FsName:      "fusey",
			AllowOther:  false,
			DirectMount: true,
		},
	})
	if err != nil {
		readyPipe.Close() // EOF on parent's read end signals mount failure
		log.Fatalf("mount: %v", err)
	}
	log.Printf("mounted at %s (daemon %s)", mountpoint, daemonID)

	// Write PID file for unmount lookup before signalling the parent, so that
	// `fusey unmount` cannot race against a daemon that has not yet written it.
	pidPath := filepath.Join(daemonDir, "daemon.pid")
	pidData, _ := json.Marshal(daemonInfo{PID: os.Getpid(), Mountpoint: mountpoint})
	_ = os.WriteFile(pidPath, append(pidData, '\n'), 0644)

	// Signal the parent: filesystem is accessible; parent will exit 0.
	_, _ = fmt.Fprint(readyPipe, "ready\n")
	readyPipe.Close()

	// Periodic index persistence — exits cleanly when ctx is cancelled.
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
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer flushCancel()
	if err := persistFn(flushCtx); err != nil {
		log.Printf("final persist: %v", err)
	}
	_ = os.Remove(pidPath)
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
func runCompact() {
	cfg := mustLoadConfig()
	ctx := context.Background()

	objStore, cs := mustBuildStore(ctx, cfg)
	idx := loadIndex(ctx, cfg, objStore)

	persistFn := buildPersistFn(cfg, idx, cs, objStore)
	comp := compaction.New(idx, cs, persistFn, cfg.CompactionThreshold, cfg.ChunkSize)

	log.Println("starting compaction cycle")
	if err := comp.Compact(ctx); err != nil {
		log.Fatalf("compact: %v", err)
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
//  3. store.PutRaw (remote index) — written last, and only when the index has
//     structural mutations since the last remote write (idx.IsRemoteDirty()).
//     Atime-only updates from reads do not set the remote-dirty flag, so pure
//     read workloads generate no presigned-URL round-trips.
func buildPersistFn(cfg *config.Config, idx *index.Index, cs *chunks.ChunkStore, store chunks.ObjectStore) func(context.Context) error {
	return func(ctx context.Context) error {
		if idx.IsDirty() {
			if err := index.Save(idx, cfg.CacheDir); err != nil {
				return err
			}
		}
		if err := cs.FlushActive(ctx); err != nil {
			return err
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
