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

// Retry configuration for transient HTTP failures.
const (
	defaultMaxAttempts = 4                     // 1 initial attempt + 3 retries
	baseRetryDelay     = 200 * time.Millisecond // delay before the 2nd attempt
	maxRetryDelay      = 8 * time.Second        // cap on exponential backoff
)

// retryableError wraps an error that is safe to retry. It is used internally
// to signal that a request failed with a transient condition (5xx from broker
// or storage, 403 presigned-URL expiry, or a network-level error) and the
// entire operation should be attempted again from scratch.
type retryableError struct{ cause error }

func (e *retryableError) Error() string { return e.cause.Error() }
func (e *retryableError) Unwrap() error { return e.cause }

func makeRetryable(err error) error { return &retryableError{cause: err} }

func isRetryable(err error) bool {
	var r *retryableError
	return errors.As(err, &r)
}

// unwrapRetryable strips the retryableError wrapper, returning the underlying
// error. If err is not a retryableError it is returned as-is.
func unwrapRetryable(err error) error {
	var r *retryableError
	if errors.As(err, &r) {
		return r.cause
	}
	return err
}

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
	baseURL     string
	authHeader  string
	authValue   string
	client      *http.Client
	maxAttempts int           // total attempts (1 = no retries)
	baseDelay   time.Duration // delay before the 2nd attempt; doubles each retry
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
		baseURL:     strings.TrimRight(baseURL, "/"),
		authHeader:  authHeader,
		authValue:   authValue,
		client:      &http.Client{Timeout: 30 * time.Second},
		maxAttempts: defaultMaxAttempts,
		baseDelay:   baseRetryDelay,
	}
}

// withRetry calls fn up to s.maxAttempts times. If fn returns a retryableError
// the call is retried after an exponential backoff delay (capped at
// maxRetryDelay). Any non-retryable error or a nil result terminates the loop
// immediately. If ctx is cancelled while waiting between retries, the last
// error is returned. The retryableError wrapper is always stripped before
// returning so callers see the underlying error.
func (s *BrokerStore) withRetry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < s.maxAttempts; attempt++ {
		if attempt > 0 {
			delay := s.baseDelay * (1 << uint(attempt-1))
			if delay > maxRetryDelay {
				delay = maxRetryDelay
			}
			select {
			case <-ctx.Done():
				return unwrapRetryable(lastErr)
			case <-time.After(delay):
			}
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isRetryable(lastErr) {
			return unwrapRetryable(lastErr)
		}
	}
	return unwrapRetryable(lastErr)
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
// 5xx responses are wrapped as retryableError; 4xx are permanent failures.
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
		err := fmt.Errorf("broker %s %q: status %d: %s", op, id, resp.StatusCode, body)
		if resp.StatusCode >= 500 {
			return makeRetryable(err)
		}
		return err
	}
}

// checkStorageStatus maps object-store (S3/GCS) HTTP status codes to Go errors.
// Used for responses to requests made directly to presigned URLs.
// 5xx and 403 (presigned-URL expiry) are marked retryable; the caller's
// withRetry loop will re-fetch a fresh presigned URL on the next attempt.
func checkStorageStatus(resp *http.Response, id string, op string) error {
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent, http.StatusPartialContent:
		return nil
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusForbidden:
		// The presigned URL has expired or is invalid. Marking retryable causes
		// the operation-level retry loop to re-fetch a fresh URL from the broker.
		return makeRetryable(fmt.Errorf("storage %s %q: forbidden (presigned URL expired)", op, id))
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		err := fmt.Errorf("storage %s %q: status %d: %s", op, id, resp.StatusCode, body)
		if resp.StatusCode >= 500 {
			return makeRetryable(err)
		}
		return err
	}
}

// uploadURL asks the broker for a presigned PUT URL for the given object id.
// Network errors are returned as retryableError so the operation-level retry
// loop will try again; auth errors from the broker are permanent failures.
func (s *BrokerStore) uploadURL(ctx context.Context, id string) (string, error) {
	req, err := s.newRequest(ctx, http.MethodGet, s.objectURL(id)+"/upload-url", nil)
	if err != nil {
		return "", fmt.Errorf("broker upload-url %q: %w", id, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", makeRetryable(fmt.Errorf("broker upload-url %q: %w", id, err))
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
// Network errors are returned as retryableError; auth/not-found are permanent.
func (s *BrokerStore) downloadURL(ctx context.Context, id string) (string, error) {
	req, err := s.newRequest(ctx, http.MethodGet, s.objectURL(id)+"/download-url", nil)
	if err != nil {
		return "", fmt.Errorf("broker download-url %q: %w", id, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", makeRetryable(fmt.Errorf("broker download-url %q: %w", id, err))
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
// The entire two-phase operation (get presigned URL + upload) is retried as a
// unit so that a 403 from the storage tier (expired presign) causes a fresh
// URL to be fetched from the broker on the next attempt.
func (s *BrokerStore) Put(ctx context.Context, id string, data []byte) error {
	return s.withRetry(ctx, func() error {
		url, err := s.uploadURL(ctx, id)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("broker put %q: %w", id, err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = int64(len(data))
		resp, err := s.client.Do(req)
		if err != nil {
			return makeRetryable(fmt.Errorf("broker put %q: %w", id, err))
		}
		defer resp.Body.Close()
		return checkStorageStatus(resp, id, "put")
	})
}

// GetRange obtains a presigned GET URL from the broker and downloads length
// bytes starting at offset directly from the object store.
// The entire two-phase operation is retried as a unit for the same reason as Put.
func (s *BrokerStore) GetRange(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	var result []byte
	err := s.withRetry(ctx, func() error {
		url, err := s.downloadURL(ctx, id)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("broker get %q: %w", id, err)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		resp, err := s.client.Do(req)
		if err != nil {
			return makeRetryable(fmt.Errorf("broker get %q: %w", id, err))
		}
		defer resp.Body.Close()
		if err := checkStorageStatus(resp, id, "get"); err != nil {
			return err
		}
		result, err = io.ReadAll(resp.Body)
		return err
	})
	return result, err
}

// Delete removes the object with the given id via the broker.
// Missing objects are not an error.
func (s *BrokerStore) Delete(ctx context.Context, id string) error {
	return s.withRetry(ctx, func() error {
		req, err := s.newRequest(ctx, http.MethodDelete, s.objectURL(id), nil)
		if err != nil {
			return fmt.Errorf("broker delete %q: %w", id, err)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			return makeRetryable(fmt.Errorf("broker delete %q: %w", id, err))
		}
		defer resp.Body.Close()
		if err := checkBrokerStatus(resp, id, "delete"); err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		return nil
	})
}

// List returns the IDs of all chunk objects via the broker.
// The index key ("index.cbor") is excluded so it is never treated as a chunk
// by the compactor.
func (s *BrokerStore) List(ctx context.Context) ([]string, error) {
	var result []string
	err := s.withRetry(ctx, func() error {
		req, err := s.newRequest(ctx, http.MethodGet, s.baseURL+"/objects", nil)
		if err != nil {
			return fmt.Errorf("broker list: %w", err)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			return makeRetryable(fmt.Errorf("broker list: %w", err))
		}
		defer resp.Body.Close()
		if err := checkBrokerStatus(resp, "", "list"); err != nil {
			return err
		}
		var ids []string
		if err := json.NewDecoder(resp.Body).Decode(&ids); err != nil {
			return fmt.Errorf("broker list: decode: %w", err)
		}
		// Filter the reserved index key; the broker may or may not exclude it.
		out := ids[:0]
		for _, id := range ids {
			if id != brokerIndexKey {
				out = append(out, id)
			}
		}
		result = out
		return nil
	})
	return result, err
}

// Size returns the byte count of the object with id via a broker HEAD request.
func (s *BrokerStore) Size(ctx context.Context, id string) (int64, error) {
	var sz int64
	err := s.withRetry(ctx, func() error {
		req, err := s.newRequest(ctx, http.MethodHead, s.objectURL(id), nil)
		if err != nil {
			return fmt.Errorf("broker size %q: %w", id, err)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			return makeRetryable(fmt.Errorf("broker size %q: %w", id, err))
		}
		defer resp.Body.Close()
		if err := checkBrokerStatus(resp, id, "size"); err != nil {
			return err
		}
		if resp.ContentLength < 0 {
			return fmt.Errorf("broker size %q: missing Content-Length", id)
		}
		sz = resp.ContentLength
		return nil
	})
	return sz, err
}

// PutRaw writes raw bytes to an arbitrary key via the presigned upload path.
// Used for index persistence.
func (s *BrokerStore) PutRaw(ctx context.Context, key string, data []byte) error {
	return s.Put(ctx, key, data)
}

// GetRaw reads the full content of an arbitrary key via the presigned download
// path. Used for index recovery. Returns ErrNotFound if the key does not exist.
// The two-phase operation is retried as a unit for the same reason as GetRange.
func (s *BrokerStore) GetRaw(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	err := s.withRetry(ctx, func() error {
		url, err := s.downloadURL(ctx, key)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("broker get raw %q: %w", key, err)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			return makeRetryable(fmt.Errorf("broker get raw %q: %w", key, err))
		}
		defer resp.Body.Close()
		if err := checkStorageStatus(resp, key, "get raw"); err != nil {
			return err
		}
		result, err = io.ReadAll(resp.Body)
		return err
	})
	return result, err
}

// IndexKey returns the reserved object ID used to store the index snapshot.
func (s *BrokerStore) IndexKey() string {
	return brokerIndexKey
}
