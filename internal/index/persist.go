package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const snapshotFile = "index.json"

// Save serialises the index to a JSON file in dir, then atomically replaces
// the previous snapshot using a rename so a partial write never corrupts it.
func Save(idx *Index, dir string) error {
	snap := idx.Snapshot()
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	tmp := filepath.Join(dir, snapshotFile+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	dst := filepath.Join(dir, snapshotFile)
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}
	idx.MarkClean()
	return nil
}

// Load reads the JSON snapshot from dir and returns a restored Index.
// Returns os.ErrNotExist (unwrapped) if no snapshot exists yet.
func Load(dir string, blockSize int32) (*Index, error) {
	path := filepath.Join(dir, snapshotFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err // caller checks os.IsNotExist
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return FromSnapshot(&snap, blockSize), nil
}
