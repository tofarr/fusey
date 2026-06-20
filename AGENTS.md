# AGENTS.md — Fusey project context

## What this project is

Fusey is a FUSE filesystem that stores data as append-only packed chunk objects
in an object store (S3 or compatible), designed for fast-resume use cases where
thousands of independent filesystem instances must attach to warm pods rather
than starting cold.

Core design: log-structured storage (Haystack / Bitcask pattern) where:
- File data → packed into sealed chunk objects in S3 (range-GET for reads)
- Metadata → in-memory index (inodes, extents, dirEntries) cached to disk
- Writes → append to active chunk, write-through to index
- Deletes/overwrites → orphan old extents, background compaction reclaims space

## Repository layout

```
specs/              Quint formal specifications (written first, before Go)
  types.qnt         Shared types: Inode, Extent, DirEntry, Chunk, IndexSnapshot
  index.qnt         Index state machine: mutations, persistence, invariants
  chunks.qnt        Chunk store: append, seal, rotate, readRange
  compaction.qnt    Compaction: target selection, remap, commit, safety proof
  filesystem.qnt    Full POSIX operations composing index + chunks
cmd/fusey/          Go binary entry point (to be implemented)
internal/
  config/           Environment variable parsing (FUSEY_* vars)
  index/            In-memory index + disk persistence
  chunks/           Active chunk buffer + S3 sealed chunk I/O
  fuse/             FUSE layer (go-fuse bindings)
  compaction/       Background compactor goroutine
```

## Key design decisions

1. **Specs before code**: Quint specs in `specs/` are the source of truth for
   invariants and operation semantics. The Go implementation must satisfy them.

2. **Go module**: `github.com/tofarr/fusey`

3. **FUSE library**: use `github.com/hanwen/go-fuse/v2` (go-fuse). It has a
   stable v2 API and good documentation. Alternative: `bazil.org/fuse`.

4. **Index structure**: The index lives entirely in memory at runtime. It is a
   flat map of inode number → Inode plus a set of DirEntry records and a map of
   inode → []Extent. This lets all metadata operations (lookup, stat, readdir)
   complete without any I/O.

5. **Disk cache format**: Serialize the IndexSnapshot type (see types.qnt) using
   `encoding/json` or `github.com/vmihaiela/msgpack` for the on-disk cache.
   The cache file lives in `FUSEY_CACHE_DIR/index.bin`.

6. **Chunk IDs**: Sequential integers formatted as zero-padded strings
   (`"chunk-00000042"`). Chosen for sortability and debuggability.

7. **Write path**: Every write appends to the active chunk buffer in memory,
   then flushes to S3 (or local cache) before returning to the kernel. The
   index is updated in memory immediately and persisted asynchronously.

8. **Environment variables** (all prefixed `FUSEY_`):
   - `FUSEY_CHUNK_SIZE` (default 64 MiB) — max chunk object size
   - `FUSEY_BLOCK_SIZE` (default 4096) — blksize reported to kernel
   - `FUSEY_CACHE_DIR` (default `/var/cache/fusey`) — on-disk index cache
   - `FUSEY_BUCKET` — S3 bucket name (required)
   - `FUSEY_ENDPOINT` — S3 endpoint URL (required)
   - `FUSEY_COMPACTION_THRESHOLD` (default 0.3) — orphan fraction trigger
   - `FUSEY_COMPACTION_INTERVAL` (default 300s)
   - `FUSEY_PERSIST_INTERVAL` (default 30s)

9. **Invariants to preserve** (from specs, enforced in implementation):
   - `nlinkConsistency`: nlink == number of DirEntries pointing at inode (+1 for dirs)
   - `extentSizeConsistency`: sum of extent lengths == inode.size for regular files
   - `onlyOneActiveChunk`: exactly one chunk has status Active at any time
   - `activeChunkBound`: active chunk size <= FUSEY_CHUNK_SIZE
   - `lastExtentReadable`: last written extent is always within its chunk's bounds
   - `allLiveExtentsPreserved` (compaction): no live extent lost during compaction

10. **Crash safety** (from compaction.qnt commitCompaction):
    - Index must be persisted before old chunks are deleted
    - Recovering from crash-before-delete: old chunks still exist, re-deleting is idempotent
    - Recovering from crash-before-persist: replay from backing store index (not disk cache)

## Quint spec notes

- Specs use module-level `const` for configuration (BLOCK_SIZE, MAX_CHUNK_SIZE, ROOT_INO).
  These are instantiated at model-check time via `--const` flags.
- `filesystem.qnt` re-declares all state from sub-modules (Quint doesn't share
  mutable state across modules). The implementation wires these together through
  a single top-level `FS` struct.
- Some Quint syntax in the specs may need minor corrections when run against
  the actual Quint compiler — treat the specs as the authoritative model of
  intent, not line-for-line valid syntax without checking.
- `choose()` on a set selects an arbitrary element (Quint built-in).
- Map operations used: `.set(k,v)`, `.get(k)`, `.remove(k)`, `.keys()`, `.values()`.
- List operations used: `.append(x)`, `.foldl(init, f)`, `.filter(f)`, `.map(f)`, `.nth(i)`.

## Implementation order

1. `internal/config` — parse all FUSEY_* env vars into a Config struct
2. `internal/index` — in-memory index + JSON serialisation for disk cache
3. `internal/chunks` — active chunk write buffer + S3 get/put wrapper
4. `internal/fuse` — FUSE operations (start with lookup, getattr, readdir, read)
5. `internal/compaction` — background goroutine
6. `cmd/fusey` — main binary: mount path and bucket from CLI args

## Build / test

```bash
go build ./...
go test ./...
quint specs/filesystem.qnt          # type check
quint verify specs/filesystem.qnt   # model check (requires Apalache)
```
