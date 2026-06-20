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

	s3store, cs := mustBuildStore(ctx, cfg)
	idx := loadIndex(ctx, cfg, s3store)

	persistFn := buildPersistFn(ctx, cfg, idx, s3store)

	f := fusefs.New(idx, cs, cfg.MaxFSSize, cfg.CacheDir)
	server, err := gofs.Mount(mountpoint, f.Root(), &gofs.Options{
		MountOptions: fuse.MountOptions{
			FsName:     "fusey",
			AllowOther: false,
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

	s3store, cs := mustBuildStore(ctx, cfg)
	idx := loadIndex(ctx, cfg, s3store)

	persistFn := buildPersistFn(ctx, cfg, idx, s3store)
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
	if cfg.Bucket == "" {
		log.Fatal("FUSEY_BUCKET is required")
	}
	return cfg
}

func mustBuildStore(ctx context.Context, cfg *config.Config) (*chunks.S3Store, *chunks.ChunkStore) {
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
	return s3store, chunks.NewChunkStore(s3store, cfg.ChunkSize)
}

// buildPersistFn returns a function that writes the index to local disk then S3.
func buildPersistFn(ctx context.Context, cfg *config.Config, idx *index.Index, s3store *chunks.S3Store) func() error {
	return func() error {
		if err := index.Save(idx, cfg.CacheDir); err != nil {
			return err
		}
		data, err := index.Marshal(idx)
		if err != nil {
			return err
		}
		return s3store.PutRaw(ctx, s3store.IndexKey(), data)
	}
}

// loadIndex tries to restore the index from (in order):
//  1. Local disk cache — fastest path, used on warm restarts of the same pod.
//  2. S3 — used when the local cache is absent (new pod, warm pod takeover).
//  3. Empty index — genuinely fresh filesystem (first ever mount).
func loadIndex(ctx context.Context, cfg *config.Config, s3store *chunks.S3Store) *index.Index {
	idx, err := index.Load(cfg.CacheDir, cfg.BlockSize)
	if err == nil {
		log.Printf("loaded index from local cache %s", cfg.CacheDir)
		return idx
	}
	if !os.IsNotExist(err) {
		log.Fatalf("load index from disk: %v", err)
	}

	data, err := s3store.GetRaw(ctx, s3store.IndexKey())
	if err == nil {
		idx, err = index.Unmarshal(data, cfg.BlockSize)
		if err != nil {
			log.Fatalf("parse index from S3: %v", err)
		}
		log.Printf("loaded index from S3 (%s/%s)", cfg.Bucket, s3store.IndexKey())
		if saveErr := index.Save(idx, cfg.CacheDir); saveErr != nil {
			log.Printf("warn: could not cache index locally: %v", saveErr)
		}
		return idx
	}
	if !errors.Is(err, chunks.ErrNotFound) {
		log.Fatalf("load index from S3: %v", err)
	}

	log.Printf("no existing index found; starting fresh filesystem")
	return index.New(cfg.BlockSize)
}
