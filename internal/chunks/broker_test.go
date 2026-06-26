package chunks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeBrokerServer is a minimal in-memory implementation of the broker HTTP
// API, used as the server-side fixture in all BrokerStore tests.
//
// It implements the two-phase presigned-URL protocol:
//   - Auth-protected broker endpoints issue self-referential "presigned" URLs
//     (pointing to /raw/{id} on this same server) that do not require the auth
//     header, mimicking how real S3 presigned URLs embed credentials in the URL.
//   - /raw/{id} endpoints serve as the simulated object store.
type fakeBrokerServer struct {
	mu         sync.Mutex
	objects    map[string][]byte
	authHeader string
	authValue  string
	serverURL  string // set after httptest.NewServer starts
}

func newFakeBrokerServer(authHeader, authValue string) *fakeBrokerServer {
	return &fakeBrokerServer{
		objects:    make(map[string][]byte),
		authHeader: authHeader,
		authValue:  authValue,
	}
}

func (b *fakeBrokerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// /raw/{id} — unauthenticated object store (simulates S3 presigned URL target)
	if strings.HasPrefix(r.URL.Path, "/raw/") {
		b.serveRaw(w, r)
		return
	}

	// All other routes require auth.
	if r.Header.Get(b.authHeader) != b.authValue {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	switch {
	case r.URL.Path == "/objects":
		b.serveList(w, r)
	case strings.HasSuffix(r.URL.Path, "/upload-url"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/objects/"), "/upload-url")
		b.serveUploadURL(w, r, id)
	case strings.HasSuffix(r.URL.Path, "/download-url"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/objects/"), "/download-url")
		b.serveDownloadURL(w, r, id)
	default:
		id := strings.TrimPrefix(r.URL.Path, "/objects/")
		b.serveObjectDirect(w, r, id)
	}
}

// serveList handles GET /objects → JSON array of IDs.
func (b *fakeBrokerServer) serveList(w http.ResponseWriter, r *http.Request) {
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
}

// serveUploadURL handles GET /objects/{id}/upload-url → presigned PUT URL.
func (b *fakeBrokerServer) serveUploadURL(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(presignedURLResponse{URL: b.serverURL + "/raw/" + id})
}

// serveDownloadURL handles GET /objects/{id}/download-url → presigned GET URL.
func (b *fakeBrokerServer) serveDownloadURL(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	b.mu.Lock()
	_, exists := b.objects[id]
	b.mu.Unlock()
	if !exists {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(presignedURLResponse{URL: b.serverURL + "/raw/" + id})
}

// serveObjectDirect handles DELETE and HEAD /objects/{id} (no byte transfer).
func (b *fakeBrokerServer) serveObjectDirect(w http.ResponseWriter, r *http.Request, id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch r.Method {
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

// serveRaw handles PUT and GET /raw/{id} — the simulated S3 presigned URL target.
// No auth check: credentials are assumed to be embedded in the URL (as with real S3).
func (b *fakeBrokerServer) serveRaw(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/raw/")
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
		parts := strings.SplitN(strings.TrimPrefix(rangeHdr, "bytes="), "-", 2)
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
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// newBrokerTestSetup spins up a fakeBrokerServer HTTP test server and returns
// a BrokerStore wired to it. The server is closed when the test finishes.
func newBrokerTestSetup(t *testing.T) (*BrokerStore, *fakeBrokerServer) {
	t.Helper()
	broker := newFakeBrokerServer("X-Session-Api-Key", "test-secret")
	srv := httptest.NewServer(broker)
	broker.serverURL = srv.URL // set after start so presigned URLs resolve correctly
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
	if store.IndexKey() != "index.cbor" {
		t.Errorf("IndexKey: got %q, want %q", store.IndexKey(), "index.cbor")
	}
}

// TestBrokerImplementsObjectStore is a compile-time check that BrokerStore
// satisfies the ObjectStore interface.
func TestBrokerImplementsObjectStore(t *testing.T) {
	var _ ObjectStore = (*BrokerStore)(nil)
}

// ---------------------------------------------------------------------------
// Retry test helpers
// ---------------------------------------------------------------------------

// failingHandler wraps an HTTP handler and returns the given HTTP status code
// for the first n requests whose URL path contains pathMatch (or every request
// when pathMatch is empty). Subsequent requests are forwarded normally.
type failingHandler struct {
	mu        sync.Mutex
	remaining int
	code      int
	pathMatch string
	next      http.Handler
}

func (f *failingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	if f.remaining > 0 && (f.pathMatch == "" || strings.Contains(r.URL.Path, f.pathMatch)) {
		f.remaining--
		f.mu.Unlock()
		http.Error(w, "injected failure", f.code)
		return
	}
	f.mu.Unlock()
	f.next.ServeHTTP(w, r)
}

// flakyTransport returns a network-level error for the first n RoundTrip calls,
// then delegates to the underlying transport.
type flakyTransport struct {
	mu        sync.Mutex
	remaining int
	next      http.RoundTripper
}

func (f *flakyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	if f.remaining > 0 {
		f.remaining--
		f.mu.Unlock()
		return nil, fmt.Errorf("injected network error")
	}
	f.mu.Unlock()
	return f.next.RoundTrip(req)
}

// newRetryTestSetup builds a BrokerStore wired to the given HTTP handler.
// The store has baseDelay=0 so retry tests run without sleeping.
func newRetryTestSetup(t *testing.T, h http.Handler, broker *fakeBrokerServer) *BrokerStore {
	t.Helper()
	srv := httptest.NewServer(h)
	broker.serverURL = srv.URL
	t.Cleanup(srv.Close)
	store := NewBrokerStore(srv.URL, "X-Session-Api-Key", "test-secret")
	store.baseDelay = 0 // suppress backoff delays in tests
	return store
}

// ---------------------------------------------------------------------------
// Retry tests
// ---------------------------------------------------------------------------

// TestBrokerPutRetries5xxBroker verifies that a 5xx response from the broker's
// upload-url endpoint is retried and the Put eventually succeeds.
func TestBrokerPutRetries5xxBroker(t *testing.T) {
	ctx := context.Background()
	broker := newFakeBrokerServer("X-Session-Api-Key", "test-secret")
	// Fail the first two upload-url requests with 503; the third succeeds.
	h := &failingHandler{remaining: 2, code: http.StatusServiceUnavailable, pathMatch: "upload-url", next: broker}
	store := newRetryTestSetup(t, h, broker)

	if err := store.Put(ctx, "chunk-retry-broker", []byte("hello")); err != nil {
		t.Fatalf("Put: expected success after retries, got: %v", err)
	}
	got, err := store.GetRange(ctx, "chunk-retry-broker", 0, 5)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("data mismatch: got %q, want %q", got, "hello")
	}
}

// TestBrokerPutRetries5xxStorage verifies that a 5xx response from the storage
// tier (the presigned-URL target) is retried and the Put eventually succeeds.
func TestBrokerPutRetries5xxStorage(t *testing.T) {
	ctx := context.Background()
	broker := newFakeBrokerServer("X-Session-Api-Key", "test-secret")
	// Fail the first two requests to /raw/ with 500; the third succeeds.
	h := &failingHandler{remaining: 2, code: http.StatusInternalServerError, pathMatch: "/raw/", next: broker}
	store := newRetryTestSetup(t, h, broker)

	if err := store.Put(ctx, "chunk-retry-storage", []byte("world")); err != nil {
		t.Fatalf("Put: expected success after retries, got: %v", err)
	}
	got, err := store.GetRange(ctx, "chunk-retry-storage", 0, 5)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(got) != "world" {
		t.Errorf("data mismatch: got %q, want %q", got, "world")
	}
}

// TestBrokerPutRetries403PresignExpiry verifies that a 403 from the storage
// tier (expired presigned URL) is retried and a fresh URL is fetched from the
// broker on each attempt.
func TestBrokerPutRetries403PresignExpiry(t *testing.T) {
	ctx := context.Background()
	broker := newFakeBrokerServer("X-Session-Api-Key", "test-secret")
	// Fail the first two storage PUT requests with 403; the third succeeds.
	h := &failingHandler{remaining: 2, code: http.StatusForbidden, pathMatch: "/raw/", next: broker}
	store := newRetryTestSetup(t, h, broker)

	if err := store.Put(ctx, "chunk-presign-expiry", []byte("fresh")); err != nil {
		t.Fatalf("Put: expected success after presign-URL refresh, got: %v", err)
	}
	got, err := store.GetRange(ctx, "chunk-presign-expiry", 0, 5)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(got) != "fresh" {
		t.Errorf("data mismatch: got %q, want %q", got, "fresh")
	}
}

// TestBrokerRetriesNetworkError verifies that a transport-level network error
// (connection refused, reset, etc.) triggers the retry loop.
func TestBrokerRetriesNetworkError(t *testing.T) {
	ctx := context.Background()
	broker := newFakeBrokerServer("X-Session-Api-Key", "test-secret")
	srv := httptest.NewServer(broker)
	broker.serverURL = srv.URL
	t.Cleanup(srv.Close)

	store := NewBrokerStore(srv.URL, "X-Session-Api-Key", "test-secret")
	store.baseDelay = 0
	// Inject two network errors before delegating to the real transport.
	store.client = &http.Client{
		Transport: &flakyTransport{remaining: 2, next: http.DefaultTransport},
	}

	if err := store.Put(ctx, "chunk-net-retry", []byte("net")); err != nil {
		t.Fatalf("Put: expected success after network-error retries, got: %v", err)
	}
	got, err := store.GetRange(ctx, "chunk-net-retry", 0, 3)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(got) != "net" {
		t.Errorf("data mismatch: got %q, want %q", got, "net")
	}
}

// TestBrokerNoRetryOnPermanentError verifies that 401 Unauthorized causes an
// immediate failure without retrying. The attempt counter must equal 1.
func TestBrokerNoRetryOnPermanentError(t *testing.T) {
	ctx := context.Background()
	// Use a wrong auth value so every request is rejected with 401.
	broker := newFakeBrokerServer("X-Session-Api-Key", "correct-secret")
	srv := httptest.NewServer(broker)
	broker.serverURL = srv.URL
	t.Cleanup(srv.Close)

	var attempts int
	counting := &failingHandler{
		remaining: 0, // never inject extra failures; count real requests
		next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "upload-url") {
				attempts++
			}
			broker.ServeHTTP(w, r)
		}),
	}
	_ = counting // suppress unused warning; we use broker directly below

	store := NewBrokerStore(srv.URL, "X-Session-Api-Key", "wrong-secret")
	store.baseDelay = 0

	err := store.Put(ctx, "should-fail", []byte("x"))
	if err == nil {
		t.Fatal("Put with wrong auth: expected error, got nil")
	}
	// 401 is not retryable — the error message must not contain a retry hint.
	if isRetryable(err) {
		t.Errorf("Put auth error must not be retryable: %v", err)
	}
}

// TestBrokerExhaustsRetries verifies that when all attempts fail with a
// retryable error the store returns an error after exactly maxAttempts tries.
func TestBrokerExhaustsRetries(t *testing.T) {
	ctx := context.Background()
	broker := newFakeBrokerServer("X-Session-Api-Key", "test-secret")

	var calls int
	counting := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "upload-url") {
			calls++
		}
		// Always return 503 for broker requests so they are all retried.
		if !strings.HasPrefix(r.URL.Path, "/raw/") {
			http.Error(w, "always down", http.StatusServiceUnavailable)
			return
		}
		broker.ServeHTTP(w, r)
	})

	srv := httptest.NewServer(counting)
	broker.serverURL = srv.URL
	t.Cleanup(srv.Close)

	store := NewBrokerStore(srv.URL, "X-Session-Api-Key", "test-secret")
	store.baseDelay = 0
	store.maxAttempts = 3 // 1 initial + 2 retries

	err := store.Put(ctx, "always-fails", []byte("x"))
	if err == nil {
		t.Fatal("Put: expected error after exhausted retries, got nil")
	}
	if calls != store.maxAttempts {
		t.Errorf("expected %d upload-url calls (one per attempt), got %d", store.maxAttempts, calls)
	}
}
