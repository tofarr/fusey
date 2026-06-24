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

const brokerIndexKey = "index.cbor"

// BrokerStore implements ObjectStore using a two-phase presigned-URL protocol.
// The broker holds object-store credentials; fusey authenticates with a single
// bearer token and uploads/downloads bytes directly to/from the object store,
// keeping the broker out of the data path.
//
// Broker API (all requests carry the configured auth header):
//
//	GET    /objects/{id}/upload-url   — return a presigned PUT URL for the object
//	GET    /objects/{id}/download-url — return a presigned GET URL for the object
//	DELETE /objects/{id}              — remove an object (idempotent)
//	HEAD   /objects/{id}              — return Content-Length only
//	GET    /objects                   — return a JSON array of object IDs
//
// Data flow for writes:
//  1. GET /objects/{id}/upload-url   → {"url":"https://s3…"}  (auth header required)
//  2. PUT bytes directly to the presigned URL              (no auth header; creds in URL)
//
// Data flow for reads:
//  1. GET /objects/{id}/download-url → {"url":"https://s3…"}  (auth header required)
//  2. GET the presigned URL with a Range header            (no auth header; creds in URL)
//
// The filesystem index is stored under the reserved ID "index.cbor". List()
// excludes this ID so it is never treated as a chunk by the compactor.
type BrokerStore struct {
	baseURL    string
	authHeader string
	authValue  string
	client     *http.Client
}

// presignedURLResponse is the JSON body returned by the upload-url and
// download-url broker endpoints.
type presignedURLResponse struct {
	URL string `json:"url"`
}

// NewBrokerStore constructs a BrokerStore.
// baseURL is the scheme+host+optional-path prefix of the broker (no trailing slash).
// authHeader and authValue are the header name and token sent on every broker request.
func NewBrokerStore(baseURL, authHeader, authValue string) *BrokerStore {
	return &BrokerStore{
		baseURL:    strings.TrimRight(baseURL, "/"),
		authHeader: authHeader,
		authValue:  authValue,
		client:     &http.Client{Timeout: 30 * time.Second},
	}
}

// objectURL builds the broker URL for a specific object ID.
func (s *BrokerStore) objectURL(id string) string {
	return s.baseURL + "/objects/" + id
}

// newRequest builds an authenticated request to the broker.
func (s *BrokerStore) newRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(s.authHeader, s.authValue)
	return req, nil
}

// checkBrokerStatus maps broker HTTP status codes to Go errors.
func checkBrokerStatus(resp *http.Response, id string, op string) error {
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

// checkStorageStatus maps object-store (S3/GCS) HTTP status codes to Go errors.
// Used for responses to requests made directly to presigned URLs.
func checkStorageStatus(resp *http.Response, id string, op string) error {
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent, http.StatusPartialContent:
		return nil
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusForbidden:
		// Presigned URL has expired or is invalid.
		return fmt.Errorf("storage %s %q: forbidden (presigned URL expired or invalid)", op, id)
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("storage %s %q: status %d: %s", op, id, resp.StatusCode, body)
	}
}

// uploadURL asks the broker for a presigned PUT URL for the given object id.
func (s *BrokerStore) uploadURL(ctx context.Context, id string) (string, error) {
	req, err := s.newRequest(ctx, http.MethodGet, s.objectURL(id)+"/upload-url", nil)
	if err != nil {
		return "", fmt.Errorf("broker upload-url %q: %w", id, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("broker upload-url %q: %w", id, err)
	}
	defer resp.Body.Close()
	if err := checkBrokerStatus(resp, id, "upload-url"); err != nil {
		return "", err
	}
	var p presignedURLResponse
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return "", fmt.Errorf("broker upload-url %q: decode: %w", id, err)
	}
	return p.URL, nil
}

// downloadURL asks the broker for a presigned GET URL for the given object id.
func (s *BrokerStore) downloadURL(ctx context.Context, id string) (string, error) {
	req, err := s.newRequest(ctx, http.MethodGet, s.objectURL(id)+"/download-url", nil)
	if err != nil {
		return "", fmt.Errorf("broker download-url %q: %w", id, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("broker download-url %q: %w", id, err)
	}
	defer resp.Body.Close()
	if err := checkBrokerStatus(resp, id, "download-url"); err != nil {
		return "", err
	}
	var p presignedURLResponse
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return "", fmt.Errorf("broker download-url %q: decode: %w", id, err)
	}
	return p.URL, nil
}

// Put obtains a presigned PUT URL from the broker and uploads data directly
// to the object store, bypassing the broker for the data transfer.
func (s *BrokerStore) Put(ctx context.Context, id string, data []byte) error {
	url, err := s.uploadURL(ctx, id)
	if err != nil {
		return fmt.Errorf("broker put %q: %w", id, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
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
	return checkStorageStatus(resp, id, "put")
}

// GetRange obtains a presigned GET URL from the broker and downloads length
// bytes starting at offset directly from the object store.
func (s *BrokerStore) GetRange(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	url, err := s.downloadURL(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("broker get %q: %w", id, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("broker get %q: %w", id, err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("broker get %q: %w", id, err)
	}
	defer resp.Body.Close()
	if err := checkStorageStatus(resp, id, "get"); err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

// Delete removes the object with the given id via the broker.
// Missing objects are not an error.
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
	if err := checkBrokerStatus(resp, id, "delete"); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

// List returns the IDs of all chunk objects via the broker.
// The index key ("index.cbor") is excluded so it is never treated as a chunk
// by the compactor.
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
	if err := checkBrokerStatus(resp, "", "list"); err != nil {
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

// Size returns the byte count of the object with id via a broker HEAD request.
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
	if err := checkBrokerStatus(resp, id, "size"); err != nil {
		return 0, err
	}
	if resp.ContentLength < 0 {
		return 0, fmt.Errorf("broker size %q: missing Content-Length", id)
	}
	return resp.ContentLength, nil
}

// PutRaw writes raw bytes to an arbitrary key via the presigned upload path.
// Used for index persistence.
func (s *BrokerStore) PutRaw(ctx context.Context, key string, data []byte) error {
	return s.Put(ctx, key, data)
}

// GetRaw reads the full content of an arbitrary key via the presigned download
// path. Used for index recovery. Returns ErrNotFound if the key does not exist.
func (s *BrokerStore) GetRaw(ctx context.Context, key string) ([]byte, error) {
	url, err := s.downloadURL(ctx, key)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("broker get raw %q: %w", key, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("broker get raw %q: %w", key, err)
	}
	defer resp.Body.Close()
	if err := checkStorageStatus(resp, key, "get raw"); err != nil {
		return nil, err
	}
	return io.ReadAll(resp.Body)
}

// IndexKey returns the reserved object ID used to store the index snapshot.
func (s *BrokerStore) IndexKey() string {
	return brokerIndexKey
}
