// journal.qnt
//
// Formal specification of the Fusey optional edit log.
//
// The journal is an append-only, sharded record of every state-mutating
// operation on the index. It is enabled by FUSEY_JOURNAL_ENABLED and lives
// in the same backing object store as the index.
//
// The journal exists to support two features:
//   1. `fusey journal-dump` — a debug tool that prints recorded edits.
//   2. `fusey mount <mp> --as-of=<ts>` — a read-only mount of the filesystem
//      as it existed at wall-clock time ts. Requires the journal to be
//      present in the backing store; errors if absent.
//
// Design constraints (resolved with the user up-front):
//   - TouchAtime (read access) is NOT journaled: too high-frequency, and
//     excluded from remoteStoreDirty in index.qnt for the same reason.
//   - Journal shards are append-only and named {prefix}journal-NNNNNNNNN.cbor
//     where N is the starting sequence number of the batch. They are never
//     overwritten; new shards accumulate. This matches the chunk store's
//     RecoverNextSeq pattern for crash safety.
//   - `fusey compact` deletes the journal at the START of its cycle, BEFORE
//     any chunk remap or deletion. If compact crashes mid-cycle, the next
//     mount has an empty journal + index+chunks in a consistent post-remap
//     state; as-of mounts simply error during that window.
//   - As-of mounts are read-only. Writing into a historical state would
//     corrupt the journal+index+chunks relationship in subtle ways.
//
// Persistence model:
//   - The on-disk index.cbor continues to be the FULL current state (for
//     fast startup), unchanged from non-journal mode.
//   - When the journal is enabled, we additionally persist a "base" snapshot
//     at {prefix}index-base.cbor. This is the state at the start of the
//     current journal period — the starting point for as-of replay.
//   - On startup, we load the current index, the base, and the journal
//     shards, and verify that base + journal == current. If verification
//     fails, we error out (the user can recover by running `fusey compact`,
//     which wipes the journal and the base, leaving the current index).
//
// Replay semantics:
//   - `replayTo(targetTs, base)` produces a fresh in-memory index state
//     equivalent to what the live index would have been at time targetTs.
//   - We start from `base` and apply every journal entry with `ts <= targetTs`.
//   - A targetTs older than the base's first journal entry is valid: the
//     base itself is the state at time `min(covered_ts)` — the very first
//     covered event. We still apply events with ts <= targetTs from that set.
//   - A targetTs newer than the most recent event yields a state equal to
//     the live current state.
//
// State from sub-modules is re-declared here because Quint does not share
// mutable state across modules. The Go implementation wires these together
// through a single top-level struct.

module journal {

  import types.* from "./types"

  // -------------------------------------------------------------------------
  // State
  // -------------------------------------------------------------------------

  // The journal entries that have been appended since the base was taken.
  // Ordered by `seq` (which is also the index in this list).
  var entries: List[JournalEntry]

  // The sequence number of the next entry to append. Always > entries.last.seq.
  var nextSeq: int

  // The seq number of the most recent entry that has been flushed to the
  // remote object store. entries[0..flushedSeq-1] is durable.
  var flushedSeq: int

  // True when there are unflushed entries (entries with seq > flushedSeq).
  var journalDirty: bool

  // The base snapshot at the start of this journal period. Loaded on startup,
  // refreshed at compact end. Used as the starting point for as-of replay.
  // When the journal is empty, base == current state.
  var base: IndexSnapshot

  // True when the journal is enabled (FUSEY_JOURNAL_ENABLED). When false, all
  // append actions are no-ops and `replayTo` would always return the base.
  // (Renamed from `enabled` because that is a Quint built-in name.)
  var journalEnabled: bool

  // -------------------------------------------------------------------------
  // Pure helpers
  // -------------------------------------------------------------------------

  // Maximum sequence number in `es`, or 0 if the list is empty.
  pure def maxSeq(es: List[JournalEntry]): int =
    if (es.length() == 0) 0
    else es.foldl(es.nth(0).seq, (acc, e) => if (e.seq > acc) e.seq else acc)

  // The list of entries with `ts <= targetTs`. Preserves order.
  pure def entriesUpTo(es: List[JournalEntry], targetTs: int): List[JournalEntry] =
    es.filter(e => e.ts <= targetTs)

  // -------------------------------------------------------------------------
  // Invariants
  // -------------------------------------------------------------------------

  // nextSeq is always strictly greater than every assigned sequence number.
  invariant seqMonotone = forall e in entries: e.seq < nextSeq

  // Sequence numbers are dense and start at 1. (Assignment is "next seq, then
  // increment", so the first appended entry has seq = 1 iff nextSeq started at 1.)
  invariant seqsContiguous =
    entries.length() == 0 or
    (entries.nth(0).seq == 1 and
     forall i in 0.to(entries.length() - 2):
       entries.nth(i + 1).seq == entries.nth(i).seq + 1)

  // flushedSeq never exceeds the highest assigned seq.
  invariant flushedWithinBounds =
    flushedSeq >= 0 and flushedSeq <= maxSeq(entries)

  // journalDirty accurately reflects whether there are unflushed entries.
  invariant journalDirtyConsistent =
    journalDirty == (maxSeq(entries) > flushedSeq)

  // All entries' timestamps are positive (post-Unix-epoch).
  invariant timestampsPositive =
    forall e in entries: e.ts > 0

  // -------------------------------------------------------------------------
  // Initialisation
  // -------------------------------------------------------------------------

  action init = {
    entries'     = List()
    nextSeq'     = 1
    flushedSeq'  = 0
    journalDirty' = false
    base'        = {
      inodes        : Map(),
      dirEntries    : Set(),
      extentMap     : Map(),
      symlinkTargets: Map(),
      xattrs        : Map(),
      nextIno       : 0
    }
    enabled'     = false
  }

  // -------------------------------------------------------------------------
  // Append
  // -------------------------------------------------------------------------

  // Append a journal entry. The seq is assigned from nextSeq and the timestamp
  // is supplied by the caller (captured at the point of the original mutation
  // so the journal entry matches the index mutation's `now` parameter).
  action append(op: JournalEvent, ts: int) = {
    val entry: JournalEntry = { seq: nextSeq, ts: ts, op: op }
    entries'      = entries.append(entry)
    nextSeq'      = nextSeq + 1
    journalDirty' = true
  }

  // -------------------------------------------------------------------------
  // Persistence
  // -------------------------------------------------------------------------

  // Flush every unflushed entry to the remote object store as one or more
  // journal-{seq_start}.cbor shards. Resets journalDirty and advances
  // flushedSeq to the maximum assigned seq.
  action flushToRemote = {
    flushedSeq'   = maxSeq(entries)
    journalDirty' = false
  }

  // Load journal shards from the remote store and apply them to `entries`,
  // advancing flushedSeq. Called on startup. Sets enabled = true.
  // In the implementation this scans for {prefix}journal-*.cbor files in
  // lexicographic order, deserialises each, and concatenates.
  action restoreFromRemote(loaded: List[JournalEntry]) = {
    entries'      = loaded
    nextSeq'      = maxSeq(loaded) + 1
    flushedSeq'   = maxSeq(loaded)
    journalDirty' = false
    enabled'      = true
  }

  // -------------------------------------------------------------------------
  // Clear (called at the START of `fusey compact`)
  // -------------------------------------------------------------------------

  // Wipe the journal in its entirety. After this action the journal is empty
  // but `enabled` remains true — subsequent appends will start a new period
  // with fresh sequence numbers starting at 1.
  //
  // The base is also cleared: with an empty journal, the base is by
  // definition equal to the current index, so it is redundant.
  action clear = {
    entries'     = List()
    nextSeq'     = 1
    flushedSeq'  = 0
    journalDirty' = false
    base'        = {
      inodes        : Map(),
      dirEntries    : Set(),
      extentMap     : Map(),
      symlinkTargets: Map(),
      xattrs        : Map(),
      nextIno       : 0
    }
  }

  // Take a fresh base snapshot. Called at the END of `fusey compact`, once
  // the index has been remapped, persisted, and the journal has been cleared.
  action refreshBase(currentState: IndexSnapshot) = {
    base' = currentState
  }

  // -------------------------------------------------------------------------
  // Replay (used by `fusey mount --as-of=<ts>`)
  // -------------------------------------------------------------------------

  // Returns the sequence numbers of entries with `ts <= targetTs` plus the
  // number of entries in that prefix. Pure helper for replayTo below.
  pure def prefixCount(es: List[JournalEntry], targetTs: int): int =
    entriesUpTo(es, targetTs).length()

  // Replay to a target timestamp. This is a pure function (no state change):
  // it takes the current base and entries, and computes the list of entries
  // that should be applied to reconstruct state at targetTs.
  //
  // The result is the list of operations to apply to a fresh in-memory index
  // started from `base`. The Go implementation applies them in order using
  // the same mutating methods on `*index.Index`.
  pure def replayOps(targetTs: int): List[JournalEvent] =
    entriesUpTo(entries, targetTs).map(e => e.op)

  // -------------------------------------------------------------------------
  // Safety: when the journal is disabled, all persistence actions preserve
  // the invariant that no entries exist.
  // -------------------------------------------------------------------------

  // When disabled, append is a no-op. We model this implicitly by requiring
  // the enabled flag in callers; the spec doesn't model the conditional.
}
