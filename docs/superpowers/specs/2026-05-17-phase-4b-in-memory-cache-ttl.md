# Phase 4 / Sub-spec B: in-memory cache + TTL invalidation

**Status:** Approved 2026-05-17
**Parent phase:** Phase 4 — persistent client-side cache (roadmap lines 179–249).
**Sub-spec order:** A (pathfs→fs migration, landed at `ccd37d2`) → **B (this)** → C (on-disk persistence) → D (Subscribe / version push).

## Goal

Add an in-memory cache layer that decorates the `FileSystemBackend`
interface (introduced in Sub-spec A) without changing its shape.
Three logical caches — attribute, directory listing, file data chunks
— share a single byte-cap LRU eviction policy. Invalidation on local
mutations is explicit. TTLs absorb stale entries when no local
mutation has fired. **No persistence** (Sub-spec C). **No Subscribe /
version push** (Sub-spec D). **Cache is disabled by default** in this
sub-spec; Sub-spec C flips the default to enabled once the persistence
story proves the disk side.

## Motivation

This is the headline internet-NFS feature foundation. With cache on,
a re-read of file content that hasn't changed since the last read
costs zero network round-trips on the data path; a repeated `stat`
inside the attr TTL window costs zero RTTs on the metadata path. On
a wide-area link with tens of milliseconds RTT, those eliminations
are the difference between mountie feeling "remote and laggy" and
"local-ish."

Sub-spec A intentionally built `FileSystemBackend` as the decorator
seam exactly so this layer could insert without touching either the
go-fuse adapter (`node.go`) or the gRPC translator (`backend_grpc.go`).
That insertion is the entirety of Sub-spec B.

## Architecture

```
                  go-fuse fs.NodeXXX     (pkg/client/io/node.go)
                            ↓
                  io.FileSystemBackend   (interface from Sub-spec A)
                            ↓
              ┌────────────┴────────────┐
              │  cache.cachedBackend     │   ← inserts here iff cfg.Cache.Enabled
              │  (new — pkg/client/cache)│
              └────────────┬────────────┘
                            ↓
                  io.BackendClient        (gRPC, from Sub-spec A)
```

The cache decorator is constructed once at mount time:

```go
backend := io.NewBackendClient(client, volume)
if cfg.Cache.Enabled {
    backend = cache.NewCachedBackend(backend, cache.ConfigFromClient(cfg.Cache))
}
root := io.NewMountieRoot(backend)
fs.Mount(path, root, opts)
```

When disabled, the chain is identical to Sub-spec A — zero runtime
overhead.

## Components

All new files live in `pkg/client/cache/`. Each has one clear
responsibility:

- **`backend.go`** — `cachedBackend` struct + `NewCachedBackend`
  constructor. Implements `io.FileSystemBackend` by delegating every
  method to `inner` while consulting/populating/invalidating the
  three sub-caches.
- **`store.go`** — `store[V]` generic type: a `sync.RWMutex`-guarded
  map keyed by string, with an LRU pointer list and per-entry size
  in bytes. Single mutex per store; the read path takes the read
  lock, mutators take the write lock.
- **`accountant.go`** — `accountant`: shared byte-cap tracker
  registered with all three stores. On insertion that would exceed
  cap, evicts the global LRU until under cap.
- **`attr.go`** — wraps a `store[*attrEntry]` (entry includes
  `attr *io.Attr` + `expiresAt time.Time` + a `negative bool` flag
  for ENOENT). Methods: `getAttr`, `putAttr`, `putNegative`,
  `invalidateAttr`.
- **`dir.go`** — wraps a `store[*dirEntry]` (entry includes
  `entries []io.DirEntry` + `expiresAt time.Time`). Methods:
  `getDir`, `putDir`, `invalidateDir`.
- **`data.go`** — wraps a `store[[]byte]` keyed by
  `chunkKey(path, chunkIndex)`. No TTL — entries are valid until
  explicitly invalidated or evicted. Methods: `getChunk`, `putChunk`,
  `invalidatePath`.
- **`config.go`** — `Config` struct + `ConfigFromClient` adapter.
- **`handle.go`** — `cachedHandle` wraps an `io.FileHandle` with
  `Unwrap()` returning the inner. Carries the path so Read/Write
  invalidations use the right key consistently. (Sub-spec A added
  `Unwrap()` to `FileHandle` for exactly this case.)

Plus per-file `*_test.go` testify suites and a `cachedBackend`
integration test against a `MockFileSystemBackend` (already generated
in Sub-spec A Task 1).

## Three caches: storage and eviction

**Three independent `store[V]` instances**, one per cache type, all
registered with a single `accountant`. The accountant tracks total
bytes across all stores; when an insertion would push the total
above `MaxSizeBytes`, the accountant evicts the global LRU item
across all stores until under cap.

Per-store hit/miss counters live on `store[V]` (Sub-spec D's metrics
work picks them up). Per-cache size in bytes lives on each store and
sums into the accountant.

Rationale: separate stores give each cache type its own observability
and let the implementation evolve independently (e.g., the data store
might later use chunked sparse maps; attr stays a plain map). Shared
accountant prevents one type starving another.

## TTL policy

| Cache | Default TTL | Notes |
|---|---|---|
| Attr (positive) | `AttrTTL` = 5 s | Refreshed on every `Stat`/`Lookup` hit (TTL is from-fetch, not from-last-touch) |
| Attr (negative) | `NegativeTTL` = 2 s | `Lookup` returned ENOENT |
| Dir | `DirTTL` = 5 s | Independent of attr TTL so they can be tuned separately |
| Data | ∞ until invalidated or evicted | No TTL; data is valid until a local mutation explicitly invalidates it (Subspec D's `Attr.version` adds the missing freshness signal) |

All TTLs are stored as absolute `time.Time` on the entry to avoid
clock-skew issues from repeated `time.Since` calls.

## Invalidation on local mutations

Correctness-critical. Every mutating op invalidates per the table
below. Each row is covered by a dedicated unit test.

| Op | Invalidates |
|---|---|
| `Write(fh, off, data)` | data chunks overlapping `[off, off+len(data))` for `fh.Path()`; attr for `fh.Path()` (size + mtime change) |
| `Truncate(path, size)` | **all** data chunks for `path`; attr for `path`. (Conservative drop-all rather than "chunks past `size`": Truncate may zero-extend or shrink, so every cached chunk's relationship to the new file length is suspect; dropping the rest is one extra fetch on the next Read, dropping the wrong ones is a stale-data bug.) |
| `Chmod(path, mode)` | attr for `path` |
| `Chown(path, uid, gid)` | attr for `path` |
| `Create(parent, name, …)` | dir for `parent`; attr for `parent` (mtime); also drops the negative attr for `joinPath(parent, name)` if cached |
| `Mkdir(path, mode)` | dir for `path`'s parent; attr for parent; drops negative attr for `path` |
| `Unlink(path)` | attr + all data for `path`; dir for `path`'s parent; **adds** a negative attr for `path` (so subsequent Stat is a fast ENOENT) |
| `Rmdir(path)` | attr + dir for `path`; dir for `path`'s parent; adds negative attr for `path` |
| `Rename(old, new)` | attr + data for both `old` and `new`; dir for both parents; adds negative attr for `old`; drops any negative attr for `new` |
| `Release(fh)` / `Flush(fh)` / `Fsync(fh)` | nothing (no observable state change beyond what Write already invalidated) |
| `Allocate(fh, off, size, mode)` | data chunks overlapping `[off, off+size)` for `fh.Path()`; attr for `fh.Path()` (size may grow) |
| `SetLk` / `SetLkw` / `GetLk` | nothing (lock state not cached) |
| `SetXAttr` / `RemoveXAttr` (not yet in `FileSystemBackend`) | n/a until those methods exist |

Read-only ops (`Stat`, `Lookup`, `ListDir`, `Read`, `Access`,
`StatFs`, `GetXAttr`, `Open`) **populate** the cache on miss; they
never invalidate.

## Read flow

- `Stat(path)`: positive-attr hit → return; negative-attr hit (and
  not expired) → return ENOENT directly; miss → `inner.Stat`,
  populate (positive or negative), return.
- `Lookup(parent, name)`: same shape as `Stat`, keyed on
  `joinPath(parent, name)`. Hit populates both the attr cache (under
  the joined path) and the dir entry's children if cached.
- `ListDir(path)`: hit → return; miss → `inner.ListDir`, populate,
  return. Also populates the per-entry attr cache if the underlying
  protocol carries inline attrs (not in Phase 3; carries only mode
  + name + ino — partial Attr only — so skip the attr pre-pop for
  now).
- `Read(fh, off, dest)`: compute the chunk range covering
  `[off, off+len(dest))`. For each chunk: hit → copy from cache;
  miss → call `inner.Read` with a chunk-sized request, populate,
  copy from there. Concatenate into `dest`.

Read-through, write-through. No single-flight in B — duplicate
concurrent misses on the same key both fetch and both populate; the
second populate overwrites the first. Sub-spec C may add singleflight
to avoid duplicate disk fills.

## Mkdir extra-Stat absorption

Sub-spec A's final review flagged that `Mkdir` does an extra `Stat`
after success because `proto.MkdirReply` carries only a status. The
cache layer absorbs this naturally: after a successful `inner.Mkdir`,
the node adapter calls `Stat` → cache miss → fetches → caches.
Subsequent stats hit cache. **No proto change in B.** Sub-spec D's
Subscribe protocol is the natural opportunity to revisit if eager
attr return becomes worth the wire break.

## Configuration

New `pkg/client/config/cache.go`:

```go
type CacheConfig struct {
    Enabled        bool          `mapstructure:"enabled"`
    MaxSizeBytes   int           `mapstructure:"max_size_bytes" validate:"min=0,max=68719476736"` // 64 GiB hard cap
    ChunkSizeBytes int           `mapstructure:"chunk_size_bytes" validate:"min=4096,max=16777216"` // 4 KiB – 16 MiB
    AttrTTL        time.Duration `mapstructure:"attr_ttl"`
    DirTTL         time.Duration `mapstructure:"dir_ttl"`
    NegativeTTL    time.Duration `mapstructure:"negative_ttl"`
}
```

Defaults:

- `Enabled` = `false`
- `MaxSizeBytes` = `1 << 30` (1 GiB)
- `ChunkSizeBytes` = `1 << 20` (1 MiB)
- `AttrTTL` = `5 * time.Second`
- `DirTTL` = `5 * time.Second`
- `NegativeTTL` = `2 * time.Second`

Wired through `pkg/client/config/config.go` (defaults, viper
`BindEnv`, env vars) following the `RpcConfig` / `FUSEConfig`
template. Documented in `docs/client/config.md`.

## Error handling

- Inner-error on cache miss: propagate verbatim. Do NOT cache
  positive errors; only ENOENT goes through the negative-attr cache
  explicitly.
- TTL expiry during an in-flight Read: the cache returns the cached
  chunk (it was valid when the chunk fetch started). Subsequent
  reads will see the expiry and re-fetch.
- Concurrent reads on the same path: each one reads from cache
  independently. No coordination needed in B.
- A Write racing a Read: the Read may briefly return now-stale data
  if its chunk fetch completed before the Write's invalidation
  registers. This matches the FUSE semantic (no read/write lock
  coordination at the FS layer).

## Testing

### Unit (`pkg/client/cache/*_test.go`)

One testify suite per non-trivial file:

- `store_test.go` — LRU eviction order, byte accounting, concurrent
  read safety (under `-race`).
- `accountant_test.go` — cross-store eviction picks the global LRU,
  cap math handles zero-size and multi-store interplay.
- `attr_test.go` — positive hit, negative hit, TTL expiry,
  invalidation.
- `dir_test.go` — same shape for dir entries.
- `data_test.go` — chunk-aligned reads from cache, partial-chunk
  read assembly, invalidation.
- `backend_test.go` — `cachedBackend` integration test against a
  `MockFileSystemBackend`: every method in the invalidation table
  above gets one row asserting the right invalidations fire.
- `handle_test.go` — `cachedHandle.Unwrap()` correctly reaches the
  inner handle; `Path()` matches.

### Integration

Existing `test/e2e/api/*` and `test/e2e/fs/*` suites must pass with
cache enabled. Add a test variant — set `cfg.Cache.Enabled = true`
in `test/e2e/utils/app.go` behind a `WithCache()` option, then add a
test suite (or extend an existing one) that runs the SimpleFS /
Streaming Read+Write / fio scenarios under cache-on conditions.

The existing cache-off runs continue unchanged (default-off in B).

### Perf

A final Sub-spec B task runs `task perf:bench` and
`task perf:bench:tcp` twice — once with default (cache off) and once
with `GMOUNTIE_BENCH_CACHE=1` (cache on, opt-in via env). benchstat
shows the on-vs-off deltas. Expected wins on repeated Stat / repeated
Read of the same file; expected ~zero delta on first-pass workloads.

## Out of scope (explicit)

- **Persistence** — bbolt + chunks/ on disk. Sub-spec C.
- **`Subscribe` RPC + `Attr.version` invalidation push.** Sub-spec D.
- **Negative-dir caching** — only negative-attr (per-path ENOENT) in B.
- **Cross-process cache sharing.** Defer indefinitely.
- **Encrypted-at-rest.** N/A for in-memory; defer for persistence.
- **Single-flight** for duplicate concurrent misses. Defer to C
  where the disk fill cost makes coordination worthwhile.
- **Pre-population of `ListDir` entries' attr cache.** Wait for a
  protocol that carries inline full Attrs (Sub-spec D candidate).
- **Cache hit/miss/eviction metrics.** Counter wiring lives next to
  Sub-spec D's metrics work where the observability story is whole.
- **`SetXAttr` / `RemoveXAttr`.** Those methods aren't on
  `FileSystemBackend` yet; not Sub-spec B's job to add them.

## Risk

The risk is silent correctness drift — a stale cache returning bytes
that don't match the server. Mitigations:

1. **Default-off ships safe.** Users opt in; rollback is "flip the
   bool."
2. **Per-row invalidation tests.** Every mutating op in the table
   above has a dedicated test asserting the exact invalidation set
   it triggers.
3. **Existing e2e suites must pass with cache on.** The SimpleFS,
   streaming, and fio scenarios are stress tests for correctness
   under realistic workloads; adding a cache-on variant catches
   regressions.
4. **`-race` runs on every cache unit test.** Concurrent
   read/write/invalidate is the trickiest correctness surface.

## Acceptance criteria

- [ ] `cache.NewCachedBackend(inner, cfg)` constructs a working
  decorator implementing `io.FileSystemBackend`.
- [ ] All ten cache subsystem test suites pass under `-race`.
- [ ] `task test` on the VM is green with `cfg.Cache.Enabled = false`
  (default; matches Sub-spec A behavior).
- [ ] `task test` on the VM is green with `cfg.Cache.Enabled = true`
  (correctness under cache).
- [ ] `task perf:bench` with cache-on shows positive deltas on
  repeated-read workloads (SeqRead on a second pass), within ±10 %
  of Sub-spec A on first-pass workloads.
- [ ] `docs/client/config.md` documents the six new `cache.*` keys.
- [ ] No proto changes. No new RPCs. No new BindEnv conflicts.
- [ ] Mocks regenerated for any new interfaces; mock diff is minimal.
