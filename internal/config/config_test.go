package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ChunkSize != DefaultChunkSize {
		t.Errorf("ChunkSize: got %d, want %d", cfg.ChunkSize, DefaultChunkSize)
	}
	if cfg.BlockSize != DefaultBlockSize {
		t.Errorf("BlockSize: got %d, want %d", cfg.BlockSize, DefaultBlockSize)
	}
	if cfg.MaxFSSize != DefaultMaxFSSize {
		t.Errorf("MaxFSSize: got %d, want %d", cfg.MaxFSSize, DefaultMaxFSSize)
	}
	if cfg.CacheDir != DefaultCacheDir {
		t.Errorf("CacheDir: got %q, want %q", cfg.CacheDir, DefaultCacheDir)
	}
	if cfg.CompactionThreshold != DefaultCompactionThreshold {
		t.Errorf("CompactionThreshold: got %f, want %f", cfg.CompactionThreshold, DefaultCompactionThreshold)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("FUSEY_CHUNK_SIZE", "1048576")
	t.Setenv("FUSEY_BLOCK_SIZE", "8192")
	t.Setenv("FUSEY_MAX_SIZE", "26843545600") // 25 GiB
	t.Setenv("FUSEY_CACHE_DIR", "/tmp/fusey-test")
	t.Setenv("FUSEY_BUCKET", "my-bucket")
	t.Setenv("FUSEY_ENDPOINT", "https://s3.example.com")
	t.Setenv("FUSEY_COMPACTION_THRESHOLD", "0.5")
	t.Setenv("FUSEY_PERSIST_INTERVAL", "60s")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ChunkSize != 1048576 {
		t.Errorf("ChunkSize: got %d, want 1048576", cfg.ChunkSize)
	}
	if cfg.BlockSize != 8192 {
		t.Errorf("BlockSize: got %d, want 8192", cfg.BlockSize)
	}
	if cfg.MaxFSSize != 26843545600 {
		t.Errorf("MaxFSSize: got %d, want 26843545600", cfg.MaxFSSize)
	}
	if cfg.CacheDir != "/tmp/fusey-test" {
		t.Errorf("CacheDir: got %q", cfg.CacheDir)
	}
	if cfg.Bucket != "my-bucket" {
		t.Errorf("Bucket: got %q", cfg.Bucket)
	}
	if cfg.Endpoint != "https://s3.example.com" {
		t.Errorf("Endpoint: got %q", cfg.Endpoint)
	}
	if cfg.CompactionThreshold != 0.5 {
		t.Errorf("CompactionThreshold: got %f", cfg.CompactionThreshold)
	}
	if cfg.PersistInterval != 60*time.Second {
		t.Errorf("PersistInterval: got %s", cfg.PersistInterval)
	}
}

func TestLoadBadValues(t *testing.T) {
	cases := []struct{ key, val string }{
		{"FUSEY_CHUNK_SIZE", "notanumber"},
		{"FUSEY_BLOCK_SIZE", "notanumber"},
		{"FUSEY_MAX_SIZE", "notanumber"},
		{"FUSEY_COMPACTION_THRESHOLD", "notafloat"},
		{"FUSEY_PERSIST_INTERVAL", "notaduration"},
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			t.Setenv(c.key, c.val)
			_, err := Load()
			if err == nil {
				t.Errorf("expected error for %s=%s", c.key, c.val)
			}
		})
	}
}

func TestMaxFSSizeZeroIsInvalid(t *testing.T) {
	t.Setenv("FUSEY_MAX_SIZE", "0")
	_, err := Load()
	if err == nil {
		t.Error("expected error for FUSEY_MAX_SIZE=0")
	}
}
