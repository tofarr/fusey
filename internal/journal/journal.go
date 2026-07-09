package journal

import (
	"sync"
	"time"
)

// Journal is the in-memory buffer of unflushed journal entries plus the
// metadata needed to manage them. When Enabled is false, Append is a no-op
// and IsDirty always returns false.
//
// All public methods are safe for concurrent use. The flush step itself
// happens under the same lock so concurrent Appends cannot be lost.
//
// The on-disk format is a sequence of CBOR-encoded Entry values. New shards
// are written by Shard.Flush; existing shards are read by ReadEntries (in
// persist.go).
type Journal struct {
	mu sync.Mutex

	enabled bool

	// nextSeq is the seq number to assign to the next appended entry.
	// Equal to (highest assigned seq) + 1, or 1 if the buffer is empty.
	nextSeq uint64

	// buffer is the list of entries not yet flushed to a shard. The
	// flushed batch at Shard (see persist.go) consumes these and resets
	// buffer to empty.
	buffer []Entry
}

// New returns a new Journal. Pass enabled=false for the production
// default (no journal); pass enabled=true when FUSEY_JOURNAL_ENABLED is set.
func New(enabled bool) *Journal {
	return &Journal{enabled: enabled, nextSeq: 1}
}

// Enabled reports whether journal recording is active.
func (j *Journal) Enabled() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.enabled
}

// SetEnabled toggles recording. When transitioning from true to false,
// in-flight entries are kept (a follow-up flush is still safe) but new
// Appends become no-ops.
func (j *Journal) SetEnabled(v bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.enabled = v
}

// NextSeq returns the seq that the next Append will assign. Exposed mainly
// for tests.
func (j *Journal) NextSeq() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.nextSeq
}

// Append records op with the given timestamp. When the journal is disabled
// this is a no-op and the returned assignedSeq is meaningless. The timestamp
// is captured by the caller (the same `now` used for the corresponding index
// mutation) so the journal entry's ts is consistent with the index's inode
// timestamps after replay.
//
// Appends performed after a CrashBeforeFlush recover automatically: the
// Shard.Flush is durable and replayed on startup.
func (j *Journal) Append(op Op, ts int64) (assignedSeq uint64) {
	if err := op.validate(); err != nil {
		// Defensive: callers should construct well-formed ops, but a
		// panic here is preferable to silently writing an invalid log.
		panic("journal: " + err.Error())
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.enabled {
		return 0
	}
	j.buffer = append(j.buffer, Entry{Seq: j.nextSeq, Ts: ts, Op: op})
	assignedSeq = j.nextSeq
	j.nextSeq++
	return assignedSeq
}

// Now is a small helper for callers that want the same clock source the
// rest of the daemon uses. Centralised here so tests can stub it.
func Now() int64 { return time.Now().UnixNano() }

// SetNextSeqAfter advances nextSeq past the highest seq in es. Used on
// startup after loading journal shards from the store, so that subsequent
// Appends don't reuse seq numbers that are already in the durable log.
//
// This call is destructive: the in-memory buffer is cleared (its contents
// are assumed to be the just-loaded entries) and the in-memory nextSeq is
// set to maxSeq(es) + 1.
func (j *Journal) SetNextSeqAfter(es []Entry) error {
	if len(es) == 0 {
		return nil
	}
	var maxSeq uint64
	for _, e := range es {
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.buffer = nil
	j.nextSeq = maxSeq + 1
	return nil
}

// BufferLen returns the number of unflushed entries. Exposed for tests and
// the periodic-flush logic in main.go.
func (j *Journal) BufferLen() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.buffer)
}

// IsDirty reports whether there are unflushed entries. Mirrors the
// journalDirty invariant from specs/journal.qnt.
func (j *Journal) IsDirty() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.buffer) > 0
}

// drainBuffer returns and clears the unflushed buffer. Called by
// Shard.Flush under the journal lock. Exposed within the package.
func (j *Journal) drainBuffer() []Entry {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := j.buffer
	j.buffer = nil
	return out
}
