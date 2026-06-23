// Package chunks implements the chunk store described in chunks.qnt.
// File data is stored as append-only chunk objects in a backing store.
// Exactly one chunk is Active (writable) at any time; all others are Sealed
// (immutable). Reads are served as byte-range fetches from sealed chunks.
package chunks

import "context"

// Store is the abstract backing store for chunk objects.
// Implementations include a local filesystem store (for testing), an S3
// store (for production), and a BrokerStore (for multi-tenant deployments).
// All methods must be safe for concurrent use.
type Store interface {
	// Put writes data to the object with the given id, creating it if it
	// does not exist or replacing it if it does. Callers must not rely on
	// read-modify-write atomicity; concurrent Puts for the same id have
	// last-write-wins semantics.
	Put(ctx context.Context, id string, data []byte) error

	// GetRange reads length bytes starting at offset from the object with id.
	GetRange(ctx context.Context, id string, offset, length int64) ([]byte, error)

	// Delete removes the object with the given id.
	Delete(ctx context.Context, id string) error

	// List returns the ids of all objects in the store.
	List(ctx context.Context) ([]string, error)

	// Size returns the total byte count of the object with id.
	Size(ctx context.Context, id string) (int64, error)
}

// ObjectStore extends Store with index persistence operations.
// Both S3Store and BrokerStore implement this interface; main.go uses it to
// avoid a concrete dependency on either implementation.
type ObjectStore interface {
	Store

	// PutRaw writes raw bytes to an arbitrary key (e.g. the index object).
	PutRaw(ctx context.Context, key string, data []byte) error

	// GetRaw reads the full content of an arbitrary key.
	// Returns ErrNotFound if the key does not exist.
	GetRaw(ctx context.Context, key string) ([]byte, error)

	// IndexKey returns the key used to store the filesystem index snapshot.
	IndexKey() string
}
