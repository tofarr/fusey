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
	// AllowOther defaults to true: fusey's typical deployment runs the
	// daemon as root (because the container is privileged and the
	// mounter needs CAP_SYS_ADMIN) while the consumer (agent-server)
	// runs as a non-root user. Without allow_other, the kernel rejects
	// the consumer's VFS operations on the mount.
	if cfg.AllowOther != DefaultAllowOther {
		t.Errorf("AllowOther: got %v, want %v", cfg.AllowOther, DefaultAllowOther)
	}
	if cfg.AllowOther != true {
		t.Errorf("AllowOther: got %v, want true (default must be true)", cfg.AllowOther)
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
	t.Setenv("FUSEY_ALLOW_OTHER", "false")

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
	if cfg.AllowOther != false {
		t.Errorf("AllowOther: got %v, want false (FUSEY_ALLOW_OTHER=false)", cfg.AllowOther)
	}
}

func TestLoadBadValues(t *testing.T) {
	cases := []struct{ key, val string }{
		{"FUSEY_CHUNK_SIZE", "notanumber"},
		{"FUSEY_BLOCK_SIZE", "notanumber"},
		{"FUSEY_MAX_SIZE", "notanumber"},
		{"FUSEY_COMPACTION_THRESHOLD", "notafloat"},
		{"FUSEY_PERSIST_INTERVAL", "notaduration"},
		{"FUSEY_ALLOW_OTHER", "yes"},
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

func TestLoadAllowOther(t *testing.T) {
	// `1`, `t`, `T`, `TRUE`, `true`, `True` all parse to true per
	// strconv.ParseBool. Covers the common opt-in spellings.
	for _, v := range []string{"true", "True", "TRUE", "1"} {
		t.Run("explicit_true_"+v, func(t *testing.T) {
			t.Setenv("FUSEY_ALLOW_OTHER", v)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !cfg.AllowOther {
				t.Errorf("AllowOther: got %v, want true", cfg.AllowOther)
			}
		})
	}

	for _, v := range []string{"false", "False", "FALSE", "0"} {
		t.Run("explicit_false_"+v, func(t *testing.T) {
			t.Setenv("FUSEY_ALLOW_OTHER", v)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.AllowOther {
				t.Errorf("AllowOther: got %v, want false", cfg.AllowOther)
			}
		})
	}
}
