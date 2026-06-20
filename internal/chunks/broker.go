package chunks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const brokerIndexKey = "index.json"

// BrokerStore implements ObjectStore by delegating all object operations to a
// trusted broker service over HTTP. The broker holds object-store credentials;
// this process authenticates with a single bearer token and never contacts the
// underlying object store directly.
//
// API contract (all requests carry the configured auth header):
//
//	PUT    /objects/{id}   — create or overwrite an object
//	GET    /objects/{id}   — retrieve bytes; honours the Range header
//	DELETE /objects/{id}   — remove an object (idempotent)
//	HEAD   /objects/{id}   — return Content-Length only
//	GET    /objects        — return a JSON array of object IDs
//
// The filesystem index is stored under the reserved ID "index.json". List()
// excludes this ID so it is never treated as a chunk by the compactor.
type BrokerStore struct {
	baseURL    string
	authHeader string
	authValue  string
	client     *http.Client
}

// NewBrokerStore constructs a BrokerStore.
// baseURL is the scheme+host+optional-path prefix of the broker (no trailing slash).
// authHeader and authValue are the header name and token sent on every request.
func NewBrokerStore(baseURL, authHeader, authValue string) *BrokerStore {
	return &BrokerStore{
		baseURL:    strings.TrimRight(baseURL, "/"),
		authHeader: authHeader,
		authValue:  authValue,
		client:     &http.Client{Timeout: 30 * time.Second},
	}
}

// objectURL builds the URL for a specific object ID.
func (s *BrokerStore) objectURL(id string) string {
	return s.baseURL + "/objects/" + id
}

// newRequest builds an authenticated HTTP request.
func (s *BrokerStore) newRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(s.authHeader, s.authValue)
	return req, nil
}

// checkStatus maps broker HTTP status codes to Go errors.
func checkStatus(resp *http.Response, id string, op string) error {
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent, http.StatusPartialContent:
		return nil
	case http.StatusUnauthorized:
		return fmt.Errorf("broker %s %q: unauthorized", op, id)
	case http.StatusNotFound:
		return ErrNotFound
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("broker %s %q: status %d: %s", op, id, resp.StatusCode, body)
	}
}

// Put writes data as a new object with the given id.
func (s *BrokerStore) Put(ctx context.Context, id string, data []byte) error {
	req, err := s.newRequest(ctx, http.MethodPut, s.objectURL(id), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("broker put %q: %w", id, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("broker put %q: %w", id, err)
	}
	defer resp.Body.Close()
	return checkStatus(resp, id, "put")
}

// GetRange reads length bytes starting at offset from the object with id.
func (s *BrokerStore) GetRange(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	req, err := s.newRequest(ctx, http.MethodGet, s.objectURL(id), nil)
	if err != nil {
		return nil, fmt.Errorf("broker get %q: %w", id, err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("broker get %q: %w", id, err)
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, id, "get"); err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

// Delete removes the object with the given id. Missing objects are not an error.
func (s *BrokerStore) Delete(ctx context.Context, id string) error {
	req, err := s.newRequest(ctx, http.MethodDelete, s.objectURL(id), nil)
	if err != nil {
		return fmt.Errorf("broker delete %q: %w", id, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("broker delete %q: %w", id, err)
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, id, "delete"); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

// List returns the IDs of all chunk objects. The index key ("index.json") is
// excluded so it is never mistaken for a chunk by the compactor.
func (s *BrokerStore) List(ctx context.Context) ([]string, error) {
	req, err := s.newRequest(ctx, http.MethodGet, s.baseURL+"/objects", nil)
	if err != nil {
		return nil, fmt.Errorf("broker list: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("broker list: %w", err)
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, "", "list"); err != nil {
		return nil, err
	}
	var ids []string
	if err := json.NewDecoder(resp.Body).Decode(&ids); err != nil {
		return nil, fmt.Errorf("broker list: decode: %w", err)
	}
	// Filter the reserved index key; the broker may or may not exclude it.
	out := ids[:0]
	for _, id := range ids {
		if id != brokerIndexKey {
			out = append(out, id)
		}
	}
	return out, nil
}

// Size returns the byte count of the object with id.
func (s *BrokerStore) Size(ctx context.Context, id string) (int64, error) {
	req, err := s.newRequest(ctx, http.MethodHead, s.objectURL(id), nil)
	if err != nil {
		return 0, fmt.Errorf("broker size %q: %w", id, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("broker size %q: %w", id, err)
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, id, "size"); err != nil {
		return 0, err
	}
	if resp.ContentLength < 0 {
		return 0, fmt.Errorf("broker size %q: missing Content-Length", id)
	}
	return resp.ContentLength, nil
}

// PutRaw writes raw bytes to an arbitrary key. Used for index persistence.
func (s *BrokerStore) PutRaw(ctx context.Context, key string, data []byte) error {
	return s.Put(ctx, key, data)
}

// GetRaw reads the full content of an arbitrary key. Used for index recovery.
// Returns ErrNotFound if the key does not exist.
func (s *BrokerStore) GetRaw(ctx context.Context, key string) ([]byte, error) {
	req, err := s.newRequest(ctx, http.MethodGet, s.objectURL(key), nil)
	if err != nil {
		return nil, fmt.Errorf("broker get raw %q: %w", key, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("broker get raw %q: %w", key, err)
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, key, "get raw"); err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

// IndexKey returns the reserved object ID used to store the index snapshot.
func (s *BrokerStore) IndexKey() string {
	return brokerIndexKey
}
