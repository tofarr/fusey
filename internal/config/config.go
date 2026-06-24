package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	DefaultChunkSize           int64         = 64 * 1024 * 1024       // 64 MiB
	DefaultBlockSize           int32         = 4096
	DefaultMaxFSSize           int64         = 1024 * 1024 * 1024 * 1024 // 1 TiB
	DefaultCacheDir                          = "/var/cache/fusey"
	DefaultCompactionThreshold float64       = 0.3
	DefaultPersistInterval     time.Duration = 30 * time.Second
	DefaultBrokerAuthHeader                  = "X-SESSION-API-KEY"
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

	// --- S3 / object store ---

	// Bucket is the S3 bucket name (FUSEY_BUCKET). Required.
	Bucket string
	// Endpoint is the S3-compatible endpoint URL (FUSEY_ENDPOINT).
	// Leave empty to use the default AWS regional endpoint.
	Endpoint string
	// Region is the S3 region (FUSEY_REGION, default "us-east-1").
	Region string
	// AccessKey is the S3 access key ID (FUSEY_ACCESS_KEY).
	// Leave empty to use the ambient credential chain (IAM role, env vars, etc.).
	AccessKey string
	// SecretKey is the S3 secret access key (FUSEY_SECRET_KEY).
	SecretKey string
	// ForcePathStyle forces path-style S3 URLs (FUSEY_FORCE_PATH_STYLE).
	// Required for MinIO and most self-hosted S3-compatible stores.
	ForcePathStyle bool
	// Prefix is prepended to every object key in the bucket (FUSEY_PREFIX).
	// Use this when multiple Fusey instances share a single bucket; e.g. "pod-abc/".
	// Chunks are stored as {Prefix}chunk-XXXXXXXX; the index as {Prefix}index.cbor.
	Prefix string

	// --- Broker store (alternative to direct S3) ---

	// BrokerURL is the base URL of the broker service (FUSEY_BROKER_URL).
	// When set, BrokerStore is used for all object operations instead of S3Store.
	// The broker holds object-store credentials; fusey authenticates with a
	// bearer token and never contacts the object store directly.
	// Example: "https://broker.internal/fusey"
	BrokerURL string
	// BrokerAuthHeader is the HTTP header name sent on every broker request
	// (FUSEY_BROKER_AUTH_HEADER, default "X-SESSION-API-KEY").
	BrokerAuthHeader string
	// BrokerAuthValue is the token value for the auth header
	// (FUSEY_BROKER_AUTH_VALUE).
	BrokerAuthValue string

	// --- Background tasks ---

	// CompactionThreshold is the orphan fraction above which a chunk is selected
	// for compaction by `fusey compact` (FUSEY_COMPACTION_THRESHOLD).
	CompactionThreshold float64
	// PersistInterval is how often the index is flushed to disk and S3 (FUSEY_PERSIST_INTERVAL).
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
		Region:              "us-east-1",
		AccessKey:           os.Getenv("FUSEY_ACCESS_KEY"),
		SecretKey:           os.Getenv("FUSEY_SECRET_KEY"),
		Prefix:              os.Getenv("FUSEY_PREFIX"),
		BrokerURL:           os.Getenv("FUSEY_BROKER_URL"),
		BrokerAuthHeader:    DefaultBrokerAuthHeader,
		BrokerAuthValue:     os.Getenv("FUSEY_BROKER_AUTH_VALUE"),
		CompactionThreshold: DefaultCompactionThreshold,
		PersistInterval:     DefaultPersistInterval,
	}
	if v := os.Getenv("FUSEY_BROKER_AUTH_HEADER"); v != "" {
		cfg.BrokerAuthHeader = v
	}
	if v := os.Getenv("FUSEY_REGION"); v != "" {
		cfg.Region = v
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
	if err := parseBoolEnv("FUSEY_FORCE_PATH_STYLE", &cfg.ForcePathStyle); err != nil {
		return nil, err
	}
	if err := parseFloat64Env("FUSEY_COMPACTION_THRESHOLD", &cfg.CompactionThreshold); err != nil {
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

func parseBoolEnv(key string, dst *bool) error {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = b
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
