package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	DefaultChunkSize           int64         = 64 * 1024 * 1024  // 64 MiB
	DefaultBlockSize           int32         = 4096
	DefaultMaxFSSize           int64         = 1024 * 1024 * 1024 * 1024 // 1 TiB
	DefaultCacheDir                          = "/var/cache/fusey"
	DefaultCompactionThreshold float64       = 0.3
	DefaultCompactionInterval  time.Duration = 5 * time.Minute
	DefaultPersistInterval     time.Duration = 30 * time.Second
)

// Config holds all runtime configuration resolved from FUSEY_* environment variables.
type Config struct {
	// ChunkSize is the maximum size in bytes of a single chunk object (FUSEY_CHUNK_SIZE).
	ChunkSize int64
	// BlockSize is the preferred I/O block size reported to the kernel (FUSEY_BLOCK_SIZE).
	BlockSize int32
	// MaxFSSize is the total capacity of the filesystem in bytes reported to the
	// kernel via statfs (FUSEY_MAX_SIZE). Free space = MaxFSSize - used bytes.
	MaxFSSize int64
	// CacheDir is the directory used for the on-disk index cache (FUSEY_CACHE_DIR).
	CacheDir string
	// Bucket is the object store bucket name (FUSEY_BUCKET).
	Bucket string
	// Endpoint is the object store endpoint URL (FUSEY_ENDPOINT).
	Endpoint string
	// CompactionThreshold is the orphan fraction above which a chunk is selected
	// for compaction (FUSEY_COMPACTION_THRESHOLD).
	CompactionThreshold float64
	// CompactionInterval is how often the background compactor runs (FUSEY_COMPACTION_INTERVAL).
	CompactionInterval time.Duration
	// PersistInterval is how often the index is flushed to the disk cache (FUSEY_PERSIST_INTERVAL).
	PersistInterval time.Duration
}

// Load reads FUSEY_* environment variables and returns a populated Config.
// Missing variables fall back to defaults; malformed values return an error.
func Load() (*Config, error) {
	cfg := &Config{
		ChunkSize:           DefaultChunkSize,
		BlockSize:           DefaultBlockSize,
		MaxFSSize:           DefaultMaxFSSize,
		CacheDir:            DefaultCacheDir,
		Bucket:              os.Getenv("FUSEY_BUCKET"),
		Endpoint:            os.Getenv("FUSEY_ENDPOINT"),
		CompactionThreshold: DefaultCompactionThreshold,
		CompactionInterval:  DefaultCompactionInterval,
		PersistInterval:     DefaultPersistInterval,
	}

	if err := parseInt64Env("FUSEY_CHUNK_SIZE", &cfg.ChunkSize); err != nil {
		return nil, err
	}
	if err := parseInt32Env("FUSEY_BLOCK_SIZE", &cfg.BlockSize); err != nil {
		return nil, err
	}
	if err := parseInt64Env("FUSEY_MAX_SIZE", &cfg.MaxFSSize); err != nil {
		return nil, err
	}
	if cfg.MaxFSSize <= 0 {
		return nil, fmt.Errorf("FUSEY_MAX_SIZE must be > 0")
	}
	if v := os.Getenv("FUSEY_CACHE_DIR"); v != "" {
		cfg.CacheDir = v
	}
	if err := parseFloat64Env("FUSEY_COMPACTION_THRESHOLD", &cfg.CompactionThreshold); err != nil {
		return nil, err
	}
	if err := parseDurationEnv("FUSEY_COMPACTION_INTERVAL", &cfg.CompactionInterval); err != nil {
		return nil, err
	}
	if err := parseDurationEnv("FUSEY_PERSIST_INTERVAL", &cfg.PersistInterval); err != nil {
		return nil, err
	}
	return cfg, nil
}

func parseInt64Env(key string, dst *int64) error {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = n
	return nil
}

func parseInt32Env(key string, dst *int32) error {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = int32(n)
	return nil
}

func parseFloat64Env(key string, dst *float64) error {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = f
	return nil
}

func parseDurationEnv(key string, dst *time.Duration) error {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = d
	return nil
}
