// Command fusey manages a Fusey FUSE filesystem backed by S3-compatible storage.
//
// Subcommands:
//
//	fusey mount <mountpoint>  — mount the filesystem and serve FUSE requests
//	fusey compact             — run one compaction cycle and exit
//
// All configuration is via FUSEY_* environment variables (see README).
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
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

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: fusey <mount|compact> [args]")
	}
	switch os.Args[1] {
	case "mount":
		if len(os.Args) != 3 {
			log.Fatal("usage: fusey mount <mountpoint>")
		}
		runMount(os.Args[2])
	case "compact":
		runCompact()
	default:
		log.Fatalf("unknown subcommand %q; use 'mount' or 'compact'", os.Args[1])
	}
}

// runMount mounts the FUSE filesystem and blocks until a signal is received.
func runMount(mountpoint string) {
	cfg := mustLoadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objStore, cs := mustBuildStore(ctx, cfg)
	idx := loadIndex(ctx, cfg, objStore)

	persistFn := buildPersistFn(ctx, cfg, idx, cs, objStore)

	f := fusefs.New(idx, cs, cfg.MaxFSSize, cfg.CacheDir)
	server, err := gofs.Mount(mountpoint, f.Root(), &gofs.Options{
		MountOptions: fuse.MountOptions{
			FsName:      "fusey",
			AllowOther:  false,
			DirectMount: true, // use mount(2) directly; requires CAP_SYS_ADMIN but avoids fusermount dependency
		},
	})
	if err != nil {
		log.Fatalf("mount: %v", err)
	}
	log.Printf("fusey mounted at %s", mountpoint)

	// Periodic index persistence.
	go func() {
		ticker := time.NewTicker(cfg.PersistInterval)
		defer ticker.Stop()
		for range ticker.C {
			if idx.IsDirty() {
				if err := persistFn(); err != nil {
					log.Printf("persist index: %v", err)
				}
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()

	log.Println("unmounting...")
	if err := server.Unmount(); err != nil {
		log.Printf("unmount: %v", err)
	}
	if err := persistFn(); err != nil {
		log.Printf("final persist: %v", err)
	}
	log.Println("done")
}

// runCompact loads the index from S3, runs one compaction cycle, persists the
// updated index, and exits. Intended to be called from a Kubernetes CronJob:
//
//	fusey compact
func runCompact() {
	cfg := mustLoadConfig()
	ctx := context.Background()

	objStore, cs := mustBuildStore(ctx, cfg)
	idx := loadIndex(ctx, cfg, objStore)

	persistFn := buildPersistFn(ctx, cfg, idx, cs, objStore)
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

// buildPersistFn returns a function that atomically flushes the active chunk
// and then writes the index to local disk and the object store.
//
// FlushActive is called first so the remote store is always self-consistent:
// every extent the persisted index references exists as an object in the store.
// This bounds the data-loss window to at most one FUSEY_PERSIST_INTERVAL on
// any crash, including OOM kills where no signal handler runs.
func buildPersistFn(ctx context.Context, cfg *config.Config, idx *index.Index, cs *chunks.ChunkStore, store chunks.ObjectStore) func() error {
	return func() error {
		if err := cs.FlushActive(ctx); err != nil {
			return err
		}
		if err := index.Save(idx, cfg.CacheDir); err != nil {
			return err
		}
		data, err := index.Marshal(idx)
		if err != nil {
			return err
		}
		return store.PutRaw(ctx, store.IndexKey(), data)
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
