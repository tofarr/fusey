# fusey

A FUSE filesystem that stores data as append-only packed chunk objects in an
object store (e.g. S3), designed for use cases where thousands of independent
filesystem instances must be resumed quickly by attaching to a warm process
rather than waiting for a cold server start.

## Motivation

Fusey was designed to solve a specific warm-pool problem:

- You have thousands of Kubernetes deployments, each with 0 or 1 pods and
  persistent storage.
- Scaling to 0 pods pauses the deployment; scaling to 1 resumes it.
- A pool of warm pods exists with empty storage, giving fast cold starts.
- **Resume is slow** because you must wait for a pod to start (~15 s).

The goal is to make resume just as fast as a cold start: assign a warm pod
and mount its data in-place, without copying gigabytes before the server can
serve requests.

Fusey achieves this by:

1. Storing all file data in an object store as append-only **chunk objects**.
2. Keeping a compact **index** (inode metadata + extent map) that can be pulled
   and mounted in under a second, even for a 25 GB filesystem with tens of
   thousands of files.
3. Serving file reads as **HTTP range-GETs** against chunk objects — the
   filesystem is immediately usable while data streams in on demand.
4. Running a background **compactor** that removes orphaned bytes from chunks
   after files are overwritten or deleted.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Linux kernel (VFS)                                             │
└──────────────────────────────┬──────────────────────────────────┘
                               │ FUSE protocol (/dev/fuse)
┌──────────────────────────────▼──────────────────────────────────┐
│  fusey FUSE daemon                                              │
│                                                                 │
│  ┌─────────────────────┐     ┌───────────────────────────────┐  │
│  │  Index (in memory)  │     │  Chunk store                  │  │
│  │                     │     │                               │  │
│  │  inodes             │     │  active chunk (write buffer)  │  │
│  │  dirEntries         │     │  sealed chunks → S3           │  │
│  │  extentMap          │     │                               │  │
│  │  symlinkTargets     │     │  reads → HTTP range-GET       │  │
│  │  xattrs             │     │  writes → append to active    │  │
│  └──────────┬──────────┘     └───────────────────────────────┘  │
│             │                                                   │
│  ┌──────────▼──────────┐     ┌───────────────────────────────┐  │
│  │  On-disk cache      │     │  Background compactor         │  │
│  │  (FUSEY_CACHE_DIR)  │     │  (removes orphaned chunk data)│  │
│  └─────────────────────┘     └───────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Chunk storage layout

File data is stored as a sequence of append-only chunk objects in the backing
store. Each chunk is a flat binary blob. The index records where in a chunk
each file's bytes live (an *extent*):

```
chunk-00000000:  [file A bytes 0-4095][file B bytes 0-8191][file A bytes 4096-8191]...
chunk-00000001:  [file C bytes 0-16383]...

index:
  inode 42 (file A) → extents:
    { chunk: chunk-00000000, chunkOffset:     0, length: 4096, fileOffset:    0 }
    { chunk: chunk-00000000, chunkOffset: 12288, length: 4096, fileOffset: 4096 }
  inode 43 (file B) → extents:
    { chunk: chunk-00000000, chunkOffset:  4096, length: 8192, fileOffset:    0 }
```

Writes append to the active chunk. When the active chunk reaches
`FUSEY_CHUNK_SIZE` bytes it is sealed (immutable) and a fresh active chunk is
opened. A read for any byte range in a file is satisfied by one or more HTTP
range-GET requests, one per extent that covers the requested range.

### Index persistence

The index is stored:

1. **In memory** — the primary working copy.
2. **On disk** (`FUSEY_CACHE_DIR`) — a serialised snapshot written periodically
   and at unmount. Used for fast recovery without hitting the backing store.
3. **In the backing store** — the authoritative copy written at unmount (or on
   an explicit flush). Used when the disk cache is absent (first mount on a
   new node).

### Compaction

Over time, overwrites and deletes leave orphaned bytes in sealed chunks.
The background compactor:

1. Identifies sealed chunks above an orphan-fraction threshold.
2. Reads all live extents from those chunks.
3. Writes them into a new compacted chunk.
4. Updates the index to point at the new locations.
5. Persists the index.
6. Deletes the old chunk objects.

The index is always persisted **before** old chunks are deleted, so the
process is crash-safe.

## Formal specification

The `specs/` directory contains a [Quint](https://quint-lang.org/) formal
specification of the system before any Go code is written. Quint is a
model-checking language (a modern successor to TLA+) that lets us verify
invariants and temporal properties of the design.

| File | What it specifies |
|---|---|
| `specs/types.qnt` | Shared data types (Inode, Extent, DirEntry, Chunk, …) |
| `specs/index.qnt` | Index state machine: mutations, persistence, invariants |
| `specs/chunks.qnt` | Chunk store: append, seal, rotate, read-range |
| `specs/compaction.qnt` | Compaction: target selection, remap, commit, safety |
| `specs/filesystem.qnt` | Full POSIX operations composing index + chunks |

### Running the specs

Install Quint:

```bash
npm install -g @informalsystems/quint
```

Type-check a spec:

```bash
quint specs/filesystem.qnt
```

Run the model checker against an invariant:

```bash
quint verify specs/filesystem.qnt --invariant rootInvariant
quint verify specs/filesystem.qnt --invariant nlinkConsistency
quint verify specs/filesystem.qnt --invariant extentSizeConsistency
```

Simulate a random execution:

```bash
quint run specs/filesystem.qnt
```

## Configuration

All configuration is via environment variables so the binary can be deployed
as a Kubernetes container without config files.

| Variable | Default | Description |
|---|---|---|
| `FUSEY_CHUNK_SIZE` | `67108864` (64 MiB) | Maximum size of a single chunk object |
| `FUSEY_BLOCK_SIZE` | `4096` | Preferred I/O block size reported to the kernel |
| `FUSEY_MAX_SIZE` | `1099511627776` (1 TiB) | Total filesystem capacity in bytes reported to the kernel via `statfs`. `df` and tools that check free space use this value. Set it to match the expected maximum size of the data stored in this instance (e.g. `26843545600` for 25 GiB). |
| `FUSEY_CACHE_DIR` | `/var/cache/fusey` | Directory for the on-disk index cache |
| `FUSEY_BUCKET` | _(required)_ | Object store bucket name |
| `FUSEY_ENDPOINT` | _(required)_ | Object store endpoint URL |
| `FUSEY_COMPACTION_THRESHOLD` | `0.3` | Orphan fraction above which a chunk is compacted |
| `FUSEY_COMPACTION_INTERVAL` | `300s` | How often the compactor runs |
| `FUSEY_PERSIST_INTERVAL` | `30s` | How often the index is flushed to disk cache |

## Project status

- [x] Formal specification (Quint)
- [x] Go implementation — index, chunk store, compaction, FUSE layer
- [x] Index persistence (disk cache — `FUSEY_CACHE_DIR/index.json`)
- [x] `statfs` support (`FUSEY_MAX_SIZE`)
- [ ] S3 chunk store backend (interface defined; `LocalStore` used for now)
- [ ] Index persistence to object store (for cold-start on a new node)
- [ ] Kubernetes deployment guide

## Development

### Prerequisites

- Go 1.22+
- [Quint](https://quint-lang.org/) (for spec verification)
- A FUSE-capable Linux host

### macOS

Fusey uses Linux FUSE and is not tested against macFUSE or FUSE-T.
For local development and testing on macOS, use [Lima](https://lima-vm.io/)
to run a lightweight Linux VM:

```bash
brew install lima
limactl start                 # starts a default Ubuntu VM
limactl shell default         # open a shell inside the VM
```

Lima mounts your home directory into the VM by default, so you can edit
files with your normal macOS tools and build/run inside the VM.

### Build

```bash
go build ./cmd/fusey
```

### Test

```bash
go test ./...
```

## Licence

Apache 2.0 — see [LICENSE](LICENSE).
