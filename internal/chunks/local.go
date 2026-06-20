package chunks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// LocalStore is a Store backed by a local directory. Each chunk is stored as
// a file named by its id. Intended for testing and local development.
type LocalStore struct {
	mu  sync.RWMutex
	dir string
}

// NewLocalStore creates a LocalStore rooted at dir, creating it if necessary.
func NewLocalStore(dir string) (*LocalStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("local store mkdir %s: %w", dir, err)
	}
	return &LocalStore{dir: dir}, nil
}

func (s *LocalStore) path(id string) string {
	return filepath.Join(s.dir, id)
}

func (s *LocalStore) Put(_ context.Context, id string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.path(id)
	if _, err := os.Stat(p); err == nil {
		return fmt.Errorf("chunk %q already exists", id)
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write chunk %q: %w", id, err)
	}
	return os.Rename(tmp, p)
}

func (s *LocalStore) GetRange(_ context.Context, id string, offset, length int64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, err := os.Open(s.path(id))
	if err != nil {
		return nil, fmt.Errorf("open chunk %q: %w", id, err)
	}
	defer f.Close()
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("read chunk %q at %d+%d: %w", id, offset, length, err)
	}
	return buf[:n], nil
}

func (s *LocalStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.path(id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete chunk %q: %w", id, err)
	}
	return nil
}

func (s *LocalStore) List(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("list chunks: %w", err)
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			ids = append(ids, e.Name())
		}
	}
	return ids, nil
}

func (s *LocalStore) Size(_ context.Context, id string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fi, err := os.Stat(s.path(id))
	if err != nil {
		return 0, fmt.Errorf("stat chunk %q: %w", id, err)
	}
	return fi.Size(), nil
}
