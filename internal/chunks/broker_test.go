package chunks

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeBrokerServer is a minimal in-memory implementation of the broker HTTP API,
// used as the server-side fixture in all BrokerStore tests.
type fakeBrokerServer struct {
	mu         sync.Mutex
	objects    map[string][]byte
	authHeader string
	authValue  string
}

func newFakeBrokerServer(authHeader, authValue string) *fakeBrokerServer {
	return &fakeBrokerServer{
		objects:    make(map[string][]byte),
		authHeader: authHeader,
		authValue:  authValue,
	}
}

func (b *fakeBrokerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(b.authHeader) != b.authValue {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Route: /objects or /objects/{id}
	path := strings.TrimPrefix(r.URL.Path, "/objects")
	if path == "" || path == "/" {
		// List
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		b.mu.Lock()
		ids := make([]string, 0, len(b.objects))
		for id := range b.objects {
			ids = append(ids, id)
		}
		b.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ids)
		return
	}

	id := strings.TrimPrefix(path, "/")
	b.mu.Lock()
	defer b.mu.Unlock()

	switch r.Method {
	case http.MethodPut:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}
		b.objects[id] = data
		w.WriteHeader(http.StatusCreated)

	case http.MethodGet:
		data, ok := b.objects[id]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			w.Write(data)
			return
		}
		// Parse "bytes=start-end"
		rangeHdr = strings.TrimPrefix(rangeHdr, "bytes=")
		parts := strings.SplitN(rangeHdr, "-", 2)
		if len(parts) != 2 {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start, err1 := strconv.ParseInt(parts[0], 10, 64)
		end, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil || start < 0 || end < start || int(end) >= len(data) {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		slice := data[start : end+1]
		w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(slice)

	case http.MethodDelete:
		delete(b.objects, id)
		w.WriteHeader(http.StatusNoContent)

	case http.MethodHead:
		data, ok := b.objects[id]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// newBrokerTestSetup spins up a fakeBrokerServer HTTP server and returns a BrokerStore
// wired to it. The server is closed when the test finishes.
func newBrokerTestSetup(t *testing.T) (*BrokerStore, *fakeBrokerServer) {
	t.Helper()
	broker := newFakeBrokerServer("X-Session-Api-Key", "test-secret")
	srv := httptest.NewServer(broker)
	t.Cleanup(srv.Close)
	store := NewBrokerStore(srv.URL, "X-Session-Api-Key", "test-secret")
	return store, broker
}

func TestBrokerPutAndGetRange(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	data := []byte("hello, broker world")
	if err := store.Put(ctx, "obj1", data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.GetRange(ctx, "obj1", 7, 6) // "broker"
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(got) != "broker" {
		t.Errorf("GetRange: got %q, want %q", got, "broker")
	}
}

func TestBrokerGetRangeFullObject(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	data := []byte("abcdefgh")
	store.Put(ctx, "full", data)

	got, err := store.GetRange(ctx, "full", 0, int64(len(data)))
	if err != nil {
		t.Fatalf("GetRange full: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestBrokerGetRangeNotFound(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	_, err := store.GetRange(ctx, "missing", 0, 4)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetRange missing: want ErrNotFound, got %v", err)
	}
}

func TestBrokerSize(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	data := []byte("0123456789")
	store.Put(ctx, "sized", data)

	sz, err := store.Size(ctx, "sized")
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if sz != int64(len(data)) {
		t.Errorf("Size: got %d, want %d", sz, len(data))
	}
}

func TestBrokerSizeNotFound(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	_, err := store.Size(ctx, "ghost")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Size missing: want ErrNotFound, got %v", err)
	}
}

func TestBrokerDelete(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	store.Put(ctx, "todelete", []byte("bye"))
	if err := store.Delete(ctx, "todelete"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := store.GetRange(ctx, "todelete", 0, 3)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("after delete: want ErrNotFound, got %v", err)
	}
}

func TestBrokerDeleteIdempotent(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	// Deleting a non-existent object must not return an error.
	if err := store.Delete(ctx, "never-existed"); err != nil {
		t.Errorf("Delete missing: want nil, got %v", err)
	}
}

func TestBrokerList(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	store.Put(ctx, "chunk-00000001", []byte("a"))
	store.Put(ctx, "chunk-00000002", []byte("bb"))
	store.Put(ctx, "chunk-00000003", []byte("ccc"))

	ids, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := map[string]bool{
		"chunk-00000001": true,
		"chunk-00000002": true,
		"chunk-00000003": true,
	}
	if len(ids) != len(want) {
		t.Errorf("List: got %d ids, want %d: %v", len(ids), len(want), ids)
	}
	for _, id := range ids {
		if !want[id] {
			t.Errorf("List: unexpected id %q", id)
		}
	}
}

func TestBrokerListExcludesIndexKey(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	// Store the index under the reserved key.
	store.PutRaw(ctx, store.IndexKey(), []byte(`{"v":1}`))
	store.Put(ctx, "chunk-00000001", []byte("data"))

	ids, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, id := range ids {
		if id == store.IndexKey() {
			t.Errorf("List must not return the index key %q", store.IndexKey())
		}
	}
	if len(ids) != 1 || ids[0] != "chunk-00000001" {
		t.Errorf("List: got %v, want [chunk-00000001]", ids)
	}
}

func TestBrokerPutRawGetRaw(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	payload := []byte(`{"index":"data"}`)
	if err := store.PutRaw(ctx, store.IndexKey(), payload); err != nil {
		t.Fatalf("PutRaw: %v", err)
	}

	got, err := store.GetRaw(ctx, store.IndexKey())
	if err != nil {
		t.Fatalf("GetRaw: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("GetRaw: got %q, want %q", got, payload)
	}
}

func TestBrokerGetRawNotFound(t *testing.T) {
	ctx := context.Background()
	store, _ := newBrokerTestSetup(t)

	_, err := store.GetRaw(ctx, store.IndexKey())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetRaw missing: want ErrNotFound, got %v", err)
	}
}

func TestBrokerUnauthorized(t *testing.T) {
	ctx := context.Background()
	broker := newFakeBrokerServer("X-Session-Api-Key", "correct-secret")
	srv := httptest.NewServer(broker)
	defer srv.Close()

	// Intentionally wrong auth value.
	store := NewBrokerStore(srv.URL, "X-Session-Api-Key", "wrong-secret")

	if err := store.Put(ctx, "x", []byte("data")); err == nil {
		t.Error("Put with wrong auth: want error, got nil")
	}
	if _, err := store.GetRange(ctx, "x", 0, 1); err == nil {
		t.Error("GetRange with wrong auth: want error, got nil")
	}
	if _, err := store.List(ctx); err == nil {
		t.Error("List with wrong auth: want error, got nil")
	}
	if _, err := store.Size(ctx, "x"); err == nil {
		t.Error("Size with wrong auth: want error, got nil")
	}
	if err := store.Delete(ctx, "x"); err == nil {
		t.Error("Delete with wrong auth: want error, got nil")
	}
}

func TestBrokerIndexKeyConstant(t *testing.T) {
	store, _ := newBrokerTestSetup(t)
	if store.IndexKey() != "index.json" {
		t.Errorf("IndexKey: got %q, want %q", store.IndexKey(), "index.json")
	}
}

// TestBrokerImplementsObjectStore is a compile-time check that BrokerStore
// satisfies the ObjectStore interface.
func TestBrokerImplementsObjectStore(t *testing.T) {
	var _ ObjectStore = (*BrokerStore)(nil)
}
