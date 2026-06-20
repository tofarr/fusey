// Command fusey mounts a Fusey FUSE filesystem.
//
// Usage:
//
//	fusey <mountpoint>
//
// All configuration is via FUSEY_* environment variables (see README).
package main

import (
	"context"
	"errors"
	"flag"
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
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("usage: fusey <mountpoint>")
	}
	mountpoint := flag.Arg(0)

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if cfg.Bucket == "" {
		log.Fatal("FUSEY_BUCKET is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build the S3 store (used for both chunks and index persistence).
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
	cs := chunks.NewChunkStore(s3store, cfg.ChunkSize)

	// Load index — three-step: local disk → S3 → fresh.
	idx := loadIndex(ctx, cfg, s3store)

	// Build and mount the FUSE filesystem.
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

	// persistFn writes the index to local disk then to S3.
	persistFn := func() error {
		if err := index.Save(idx, cfg.CacheDir); err != nil {
			return err
		}
		data, err := index.Marshal(idx)
		if err != nil {
			return err
		}
		return s3store.PutRaw(ctx, s3store.IndexKey(), data)
	}

	// Periodic persist goroutine.
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

	// Background compaction.
	comp := compaction.New(idx, cs, persistFn, cfg.CompactionThreshold, cfg.CompactionInterval)
	go comp.Run(ctx)

	// Wait for SIGINT/SIGTERM.
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

// loadIndex tries to restore the index from (in order):
//  1. Local disk cache — fastest, used on warm restarts.
//  2. S3 — used when the local cache is absent (new pod, warm pod takeover).
//  3. Empty index — genuinely fresh filesystem (first ever mount).
func loadIndex(ctx context.Context, cfg *config.Config, s3store *chunks.S3Store) *index.Index {
	// 1. Local disk cache.
	idx, err := index.Load(cfg.CacheDir, cfg.BlockSize)
	if err == nil {
		log.Printf("loaded index from local cache %s", cfg.CacheDir)
		return idx
	}
	if !os.IsNotExist(err) {
		log.Fatalf("load index from disk: %v", err)
	}

	// 2. S3.
	data, err := s3store.GetRaw(ctx, s3store.IndexKey())
	if err == nil {
		idx, err = index.Unmarshal(data, cfg.BlockSize)
		if err != nil {
			log.Fatalf("parse index from S3: %v", err)
		}
		log.Printf("loaded index from S3 (%s/%s)", cfg.Bucket, s3store.IndexKey())
		// Warm local cache for the next restart.
		if saveErr := index.Save(idx, cfg.CacheDir); saveErr != nil {
			log.Printf("warn: could not cache index locally: %v", saveErr)
		}
		return idx
	}
	if !errors.Is(err, chunks.ErrNotFound) {
		log.Fatalf("load index from S3: %v", err)
	}

	// 3. Fresh filesystem.
	log.Printf("no existing index found; starting fresh filesystem")
	return index.New(cfg.BlockSize)
}
