// Package chunks implements the chunk store described in chunks.qnt.
// File data is stored as append-only chunk objects in a backing store.
// Exactly one chunk is Active (writable) at any time; all others are Sealed
// (immutable). Reads are served as byte-range fetches from sealed chunks.
package chunks

import "context"

// Store is the abstract backing store for chunk objects.
// Implementations include a local filesystem store (for testing) and an S3
// store (for production). All methods must be safe for concurrent use.
type Store interface {
	// Put writes data as a new immutable object with the given id.
	// It is an error to call Put with an id that already exists.
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
