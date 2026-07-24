// Package chunks implements the chunk store described in chunks.qnt.
// File data is stored as append-only chunk objects in a backing store.
// Exactly one chunk is Active (writable) at any time; all others are Sealed
// (immutable). Reads are served as byte-range fetches from sealed chunks.
package chunks

import (
	"context"
	"fmt"
	"strings"
)

const JournalShardPrefix = "journal-"

// JournalShardName returns the canonical local filename for a journal
// shard starting at the given sequence number. Format: "journal-NNNNNNNNN"
// with 9-digit zero-pad. Exposed here (rather than in the journal
// package) so the broker and S3 store can construct keys without
// importing journal — which would create a cycle, since journal imports
// chunks to talk to ObjectStore.
func JournalShardName(seq uint64) string {
	return fmt.Sprintf("%s%09d", JournalShardPrefix, seq)
}

// ParseJournalShardName returns the starting seq for a journal shard
// filename, or (0, false) if the name is not a recognised journal shard.
func ParseJournalShardName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, JournalShardPrefix) {
		return 0, false
	}
	rest := name[len(JournalShardPrefix):]
	var seq uint64
	for _, c := range rest {
		if c < '0' || c > '9' {
			return 0, false
		}
		seq = seq*10 + uint64(c-'0')
	}
	return seq, true
}

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
	// The key is the FULL key as it should appear in the underlying
	// storage, including any prefix (use IndexKey / JournalKey rather
	// than constructing a key by hand).
	PutRaw(ctx context.Context, key string, data []byte) error

	// GetRaw reads the full content of an arbitrary key.
	// Returns ErrNotFound if the key does not exist.
	GetRaw(ctx context.Context, key string) ([]byte, error)

	// IndexKey returns the key used to store the filesystem index snapshot.
	IndexKey() string

	// JournalKey returns the full key (including prefix) for a journal shard
	// starting at the given sequence number. Used by the optional edit log.
	JournalKey(seq uint64) string

	// DeleteJournalKey removes a journal shard by full key. Used by
	// `fusey compact` to wipe the journal at the start of a cycle.
	DeleteJournalKey(ctx context.Context, key string) error

	// Prefix returns the per-bucket key prefix. Used by the journal
	// package to scope listing operations.
	Prefix() string
}
