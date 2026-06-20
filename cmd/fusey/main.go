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
	"flag"
	"log"
	"os"
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

	// Load or initialise the index.
	idx, err := index.Load(cfg.CacheDir, cfg.BlockSize)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatalf("load index: %v", err)
		}
		log.Printf("no existing index in %s; starting fresh", cfg.CacheDir)
		idx = index.New(cfg.BlockSize)
	}

	// Initialise the chunk store.
	if cfg.Bucket == "" || cfg.Endpoint == "" {
		log.Fatal("FUSEY_BUCKET and FUSEY_ENDPOINT are required")
	}
	// TODO: replace LocalStore with an S3Store once implemented.
	chunkDir := filepath.Join(cfg.CacheDir, "chunks")
	local, err := chunks.NewLocalStore(chunkDir)
	if err != nil {
		log.Fatalf("chunk store: %v", err)
	}
	cs := chunks.NewChunkStore(local, cfg.ChunkSize)

	// Build and mount the FUSE filesystem.
	f := fusefs.New(idx, cs)
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
	persistFn := func() error {
		return index.Save(idx, cfg.CacheDir)
	}
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
	ctx, cancel := context.WithCancel(context.Background())
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
	// Final persist before exit.
	if err := persistFn(); err != nil {
		log.Printf("final persist: %v", err)
	}
	log.Println("done")
}
