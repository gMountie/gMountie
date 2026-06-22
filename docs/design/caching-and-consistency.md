# gMountie Client Cache & Consistency Model

**Status:** Living document
**Last updated:** 2026-05-27

This document covers the client-side cache and the consistency model that
governs its freshness. It describes the Phase 4 subsystems; the underlying
wire protocol, sessions, and reliability primitives are documented in
[architecture.md](architecture.md) and are not repeated here.

> **Note on architecture.md §9:** that document deliberately lists
> "No client-side cache" and "No bidirectional RPCs from server to client"
> among the things the original protocol did not do. Both of those gaps
> are closed by the subsystems described here.

## 1. Where the cache sits

The cache is a decorator inserted into the client's backend chain at mount
time. The `FileSystemBackend` interface was introduced as the seam for
exactly this purpose:

```
       go-fuse fs.Node*        (pkg/client/backend/node.go)
                 ↓
       io.FileSystemBackend   (interface)
                 ↓
     ┌────────────────────────┐
     │  cache.cachedBackend   │   ← inserted when cfg.Cache.Enabled = true
     │  (pkg/client/backend/cache/) │
     └────────────────────────┘
                 ↓
       io.BackendClient        (gRPC translator, pkg/client/backend/)
                 ↓
       gRPC wire / server
```

When `Cache.Enabled = false` (opt-out), the chain is identical to the
pre-cache architecture: every FUSE op goes directly to the gRPC translator.
The decorator is constructed once at mount time:

```go
backend := io.NewBackendClient(client, volume)
if cfg.Cache.Enabled {
    backend = cache.NewCachedBackend(backend, cache.ConfigFromClient(cfg.Cache), p, grpcClient, volume)
}
```

`p` is a `*persist.Persist` (nil when `Cache.Path` is unset or persistence
is disabled for a particular test path). `grpcClient` is the live
`proto.RpcFsClient`; a nil client disables the Subscribe goroutine (test
scenarios, offline mode).

## 2. Cache tiers

There are five logical caches. Three are persistent (attr, dir, data); two
are memory-only (negative, xattr names).

### 2.1 Attribute cache

Caches the result of `Stat` and `Lookup` — the `io.Attr` struct that
corresponds to a `fuse.Attr`. Keyed by path string. Entries carry an
`expiresAt` timestamp from insert time.

- **TTL:** 5 minutes (`cache.attr_ttl`). Early versions used a 5 s TTL;
  the current 5-minute default reflects Subscribe push being the primary
  freshness signal. TTL is the safety net for out-of-band changes and
  emit-path bugs.
- **Population:** read-through on `Stat` miss and `Lookup` miss. Also
  populated on successful `Create` (the returned `Attr` is cached immediately).
- **Invalidation:** on local mutations (see §5), on Subscribe MUTATED /
  DELETED / RENAMED events (§4.4), and on revalidation detecting a version
  change (§4.3).
- **Persistence:** yes — gob-encoded `persistedAttr{Attr, Version, ExpiresAt,
  Negative}` in the `attr` bbolt bucket.

### 2.2 Directory cache

Caches the `[]io.DirEntry` result of `ListDir`. Keyed by directory path.

- **TTL:** 5 minutes (`cache.dir_ttl`). Same policy as attr.
- **Population:** read-through on `ListDir` miss.
- **Invalidation:** when the directory's parent is mutated (file creation,
  deletion, rename, mkdir, rmdir under that parent), and on Subscribe events
  affecting paths under the directory.
- **Persistence:** yes — gob-encoded `persistedDir{Entries, ExpiresAt}` in
  the `dir` bbolt bucket.

**Note:** when the cache is enabled, `ListDir` requests a *plus* listing
(streaming `ReadDir` with per-entry attrs — the READDIRPLUS pattern). Each
entry's full `Attr` primes the attr cache at the joined child path (standard
positive TTL), so the kernel's per-child LOOKUP after a readdir is served
with zero RPCs. The dir cache itself still stores only the dirent shape
(name + mode + ino); per-path attrs live exclusively in the attr cache so
invalidation has a single source of truth.

### 2.3 XAttr names cache

Per-entry xattr names are folded into the `ReadDir` reply when the cache is
enabled — gated by the `with_xattr` flag set in `ListDirRequest`, which the
client backend sets only when `cache.enabled = true`. This means a cold
`ls -la` on a directory costs one `ListDir` RPC instead of N per-file
`ListXAttr` calls, eliminating the O(N) RPC fan-out that is otherwise a
throughput cliff on high-latency links. The xattr names cache is
advisory/display-only: ACL enforcement remains server-side and kernel-native,
so the cache is served on TTL (`cache.xattr_ttl`, default 5 min) plus
Subscribe MUTATED/DELETED/RENAMED push invalidation — no per-path
revalidation is needed. The cache stores names only; `GetXAttr` values are
not cached (values are large, content-dependent, and their caching is deferred
to a future phase). Because proto3 cannot distinguish an empty `repeated` field
from an unset one, each `ReadDir` entry carries an explicit `xattr_listed` bool
alongside `xattr_names`: the client primes the cache — including an empty
"no xattrs" list, which is a valid positive hit — only when that bool is set,
and falls back to a direct `ListXAttr` otherwise.

### 2.4 Data cache

Caches file content in fixed-size chunks keyed by `(path, chunkIndex)`.
Chunk size is configurable (`cache.chunk_size_bytes`, default 1 MiB). Reads
are chunk-aligned: a `Read(off, len)` is decomposed into the set of chunks
covering `[off, off+len)`.

- **TTL:** none. Data chunks have no time-based expiry; they are valid until
  explicitly invalidated by a local mutation, a Subscribe event, or a version
  change detected during revalidation.
- **Population:** read-through on chunk miss. The inner backend is called with
  a chunk-aligned request; the result is stored in memory and on disk (if
  persistence is enabled).
- **Invalidation:** on writes, truncates, and other mutations affecting content
  (see §5). Invalidation is range-aware for `Write` and `Allocate` (only
  chunks overlapping the modified range are dropped); a `SetAttr` carrying a
  size change conservatively drops all chunks for the path.
- **Persistence:** yes — chunk bytes stored content-addressed in `chunks/`
  tree; index entries in `data_idx` bbolt bucket (see §6).

### 2.5 Negative cache

Caches ENOENT results so subsequent `Stat`/`Lookup` on non-existent paths
return immediately without hitting the server.

- **TTL:** 30 seconds (`cache.negative_ttl`). Shorter than positive TTL
  because a negative entry masking a freshly-created file is more surprising
  to users than a stale positive entry.
- **Population:** when `Stat` or `Lookup` returns `ENOENT`; also when a
  local `Unlink`, `Rmdir`, or `Rename(src→dest, src side)` succeeds; also
  when a Subscribe DELETED or RENAMED event arrives.
- **Invalidation:** on `Create`, `Mkdir`, or a `Rename` into that path; also
  on TTL expiry.
- **Persistence:** no. Negative entries are memory-only. A restart defaults to
  a fresh pass to the server for previously-negative paths.

### 2.6 Write-through semantics

The cache is **not write-back**. Writes always go through to the server first;
only after a successful `inner.Write` does the cache invalidate the
now-stale chunks. The kernel may buffer writes before issuing FUSE `Write` ops
(kernel writeback), but that is a FUSE mount option at the VFS layer and is
separate from this cache. When kernel writeback is enabled, writes become
visible to other processes only on `flush` or `close`. This is covered by the
write performance documentation.

`Fsync` and `Flush` pass through to the inner backend unchanged; they do not
interact with the cache beyond what the preceding `Write` ops already
invalidated.

## 3. Shared accountant and memory LRU

All three persistent cache types share a single `accountant` that tracks total
in-memory bytes across all stores. When an insertion would push the total over
`cache.memory_max_bytes`, the accountant evicts the globally-least-recently-used
entry across all stores until under cap. This prevents any one cache type from
starving the others.

The disk tier has its own independent `diskAccountant` tracking `chunks/` bytes,
governed by `cache.disk_max_bytes`. The two budgets are independent:

- A memory eviction leaves the entry on disk; subsequent gets hit disk and
  (optionally) re-promote to memory.
- A disk eviction drops the corresponding memory entry too (disk is the source
  of truth; a memory entry pointing at a missing chunk would cause a read error).

**Current disk eviction policy:** FIFO by lexical key order (`data_idx.First()`
returns the lexically-smallest composite `path\x00chunkIndex` key). The
`lru` and `lru_pos` bbolt buckets are provisioned in the schema (§6.2) but
access-time ordering is not yet wired; that is future work.

## 4. Consistency model

### 4.1 The version token

Every `Attr` carries a `version uint64` (field 18 in `fs.proto`). The server
populates it via `VersionFromAttr` in `pkg/server/io/version.go`:

```
version = mtime_ns * p1 + ctime_ns * p2 + size * p3   (mod 2⁶⁴)
```

where `p1`, `p2`, `p3` are three distinct large odd constants. Odd multipliers
are invertible mod 2⁶⁴, so each field contributes a full-width mix regardless
of the others' values. Multiplying by distinct constants rather than XOR-ing
prevents equal-field cancellation: with XOR, `mtime_ns == ctime_ns` after a
write collapses the version to `size << 16`, making otherwise-distinguishable
attrs alias. This mix avoids that class of collision entirely. A result of zero
is mapped to one so the sentinel `version = 0` (nil attr or older server that
doesn't set the field) is never aliased by a real attr. Clients receiving
`version = 0` always trigger a revalidation rather than serving from cache.

The formula captures every observable change: content changes mtime and
usually size; permission or ownership changes ctime.

### 4.2 Verified and unverified states

Every `cachedBackend` carries a `validityTracker` with two possible states:

- **Verified:** the Subscribe stream is healthy and has delivered at least one
  HEARTBEAT since the stream was established. Every cached entry can be served
  without an extra server round-trip (within its TTL).
- **Unverified:** the Subscribe stream is down, was never opened, or the cache
  has just been opened fresh (e.g. after a remount). Every cached entry must be
  confirmed via `GetAttrIfChanged` before being served.

The zero value of `validityTracker` is `stateUnverified` — the safe default.
The state machine is simple:

```
[constructed] → Unverified
Unverified + first HEARTBEAT received → Verified
Verified + Subscribe stream drops → Unverified
```

When `cache.subscribe_enabled = false`, the backend marks itself globally
verified at construction time. In this mode TTL is the sole freshness signal,
equivalent to the behavior before Subscribe push invalidation was added. This is an
explicit design choice: operators who disable Subscribe accept TTL-bounded
staleness in exchange for no persistent gRPC stream.

### 4.3 `GetAttrIfChanged` — lightweight revalidation

The RPC `RpcFs.GetAttrIfChanged(volume, path, known_version)` is a
shortcut revalidation call. If the server's current version matches
`known_version`, the server replies `not_modified = true` with no attrs
— a tiny reply that proves freshness at the cost of one small RTT. If the
version has changed, the server returns the fresh attrs.

The client's revalidation gate works as follows. When a read op (Stat,
Lookup, ListDir, Read) encounters a cached entry while in Unverified state,
and the path has not been individually verified yet in this disconnect epoch:

1. Call `GetAttrIfChanged(path, cachedVersion)`.
2. **`not_modified`:** trust the cached entry; mark the path verified for this
   epoch. No cache modification.
3. **Version changed:** atomically invalidate attr + data chunks + parent dir
   entry for the path *before* returning, so a fast-following `Read` cannot
   race past and hit the stale data. Repopulate the attr cache with the fresh
   attrs from the reply.
4. **ENOENT:** the path is gone on the server. Invalidate + add a negative
   attr entry.
5. **RPC error:** fall through to the inner backend (full Stat/Read from server).

Per-path verification accumulates in a `sync.Map` for the duration of the
current unverified epoch. When the Subscribe stream recovers and delivers its
first HEARTBEAT, the tracker flips to globally verified and the per-path map
is cleared.

### 4.4 The Subscribe stream

`RpcFs.Subscribe(volume)` is a server-streaming RPC. The server pushes
`SubscribeEvent` messages to every connected subscriber whenever a mutation
occurs on the named volume. The client opens one stream per mounted volume
and keeps it open in a background goroutine (`subscribeConsumer`).

**Event kinds:**

| Kind | Triggered by | Cache effect |
|---|---|---|
| `MUTATED` | Write, SetAttr, SetXAttr, Allocate, Create, Mkdir | Invalidate attr + all data chunks for `path`; invalidate dir + attr for `parent(path)` |
| `DELETED` | Unlink, Rmdir | Invalidate attr + all data chunks for `path`; invalidate dir + attr for `parent(path)`; add negative attr for `path` |
| `RENAMED` | Rename | Invalidate attr + data for both old and new paths; invalidate dirs + attrs for both parents; add negative attr for `old` |
| `HEARTBEAT` | Per-volume ticker (server-side, every 10 s by default) | No path; no cache change. After the first HEARTBEAT on a new stream, flip the validity tracker to Verified |

The HEARTBEAT has two roles: connection keepalive and "you have seen all
mutations up to this point." The client only flips to Verified after the first
HEARTBEAT — not on stream establishment alone — because the server's emit-then-
broadcast pipeline could have queued events between the point the client
subscribed and the point the first event arrives.

**Server-side event bus:** the server uses a `localEventBus` with a
`sync.Map[volume → []*subscriber]`. Each subscriber gets a buffered channel
(size `server.subscribe_buffer_size`, default 256). Fan-out is non-blocking:
a full subscriber channel is closed, causing the `SubscribeController` to
tear down that stream with a `ResourceExhausted` error. The client reconnects
in unverified mode.

**Subscribe is self-emit only.** The server emits events for every mutation
made through gMountie. Out-of-band edits to the underlying directory (SSH into
the server host and write directly, another process writing to the volume root)
are not captured. These rely on the TTL safety net. Hybrid inotify watching is
a documented future enhancement (see §10).

**Client reconnect:** on any stream error, the consumer marks the cache
unverified, sleeps with exponential backoff (1 s → 30 s cap), and reconnects.
The gRPC dial uses `WaitForReady(true)` so reconnections survive brief server
restarts without immediately returning an error.

### 4.5 Close-to-open semantics

"Close-to-open" is the POSIX-NFS consistency guarantee: writes made by one
client are visible to a second client that opens the file after the first
client closes it.

gMountie provides this property under normal operating conditions because:

1. Writes go through to the server immediately (write-through, not write-back).
2. Close/Flush is a no-op in the cache — it does not delay write visibility.
3. A subscribing second client receives a MUTATED event shortly after the
   first client's write lands on the server (within one heartbeat interval in
   steady state, or at next revalidation when Subscribe is down).

When kernel writeback is enabled on the writer's side, writes are visible to
the server only on `flush` or `close` — so the FUSE writeback mode is what
bounds write visibility, not this cache.

### 4.6 TTL as the safety net

With Subscribe push active, TTL is no longer the primary freshness mechanism;
it is the last line of defence for two cases Subscribe cannot cover:

1. **Out-of-band edits** on the server side (SSH in, `rsync` push directly to
   the volume root). Subscribe never emits for these. The TTL eventually expires
   the stale entry.
2. **Emit-path bugs** — if a server handler fails to call `eventBus.Emit`
   after a successful mutation. The TTL bounds the staleness window.

The defaults are deliberately long (5 min for attr and dir, 30 s for negative)
because Subscribe handles the common case; TTL need only bound the worst-case
lag, not the steady-state lag.

Setting a TTL to `0` disables time-based expiry for that tier entirely.
Operators who fully trust Subscribe (single-tenant servers, no out-of-band
edits expected) can set all three to `0` and rely purely on Subscribe plus
revalidation.

### 4.7 Cross-client write visibility

When two clients A and B are both mounted on the same volume:

1. Client A writes and closes the file. The server receives the write.
2. The server emits a MUTATED event on the volume's event bus.
3. Client B's Subscribe stream receives the event within milliseconds (or
   within the next heartbeat interval).
4. Client B's `subscribeConsumer` calls `invalidateAttr` + `invalidateData`
   for the path.
5. Client B's next `Read` for that path is a cache miss → fetches fresh bytes
   from the server.

If the Subscribe stream is down (network blip, reconnecting), step 3 is
delayed. The cache is in Unverified state. The first `Read` from client B
triggers a `GetAttrIfChanged` revalidation, which detects the version change
and invalidates the stale data before serving. There is no window where stale
bytes are served — the Unverified gate always fires when the stream is down.

## 5. Invalidation: local mutations

Every mutating op invalidates the exact cache slices listed below. The rule is
"be conservative": if the right answer is ambiguous (e.g. a size change may
shrink or zero-extend), drop everything for that path. An extra cache miss on the next
read is preferable to a stale-data bug.

| Operation | Invalidates |
|---|---|
| `Write(fh, off, data)` | Data chunks overlapping `[off, off+len(data))` for `fh.path`; attr for `fh.path` (mtime and size may change) |
| `SetAttr(path, in)` | When `in.Valid` includes a size change: **all** data chunks for `path` (conservative drop, success or failure — size applies first server-side). Attr for `path` is re-primed from the reply's final attrs on success, invalidated otherwise |
| `Allocate(fh, off, size, mode)` | Data chunks overlapping `[off, off+size)` for `fh.path`; attr for `fh.path` (size may grow) |
| `Create(parent, name, ...)` | Dir for `parent`; attr for `parent` (mtime changes); drops any negative attr for `joinPath(parent, name)` |
| `Mkdir(path, mode)` | Dir for `path`'s parent; attr for parent; drops any negative attr for `path` |
| `Unlink(path)` | Attr + all data for `path`; dir for `path`'s parent; **adds** negative attr for `path` |
| `Rmdir(path)` | Attr + dir for `path`; dir for `path`'s parent; adds negative attr for `path` |
| `Rename(old, new)` | Attr + data for both `old` and `new`; dir for both parents; adds negative attr for `old`; drops any negative attr for `new` |
| `SetXAttr(path, attr, ...)` / `RemoveXAttr(path, attr)` | XAttr names + attr for `path`, on success only. An xattr write changes the inode's ctime, so the cached attr version is stale too |
| `Release(fh)` / `Flush(fh)` / `Fsync(fh)` | Nothing (no observable state change beyond what `Write` already invalidated) |
| `SetLk` / `SetLkw` / `GetLk` | Nothing (lock state is not cached) |

Read-only ops (`Stat`, `Lookup`, `ListDir`, `Read`, `ListXAttr`, `Access`,
`StatFs`, `Open`) **populate** the cache on a miss; they never invalidate.
(`GetXAttr` is read-only but a pure pass-through — values are not cached.)

Invalidation is symmetric between local mutations and Subscribe events: both
paths call the same `invalidateAttr` / `invalidateData` / `invalidateDir`
methods on the sub-caches. The only difference is that Subscribe events arrive
asynchronously from a different client's mutations.

## 6. Persistence layer

### 6.1 On-disk layout

The persistence layer lives in `pkg/client/backend/cache/persist/`. Each volume gets
its own directory under `cache.path`:

```
<cache.path>/
└── <volume-name>/
    ├── LOCK              advisory flock; single-process ownership
    ├── meta.db           bbolt database (NoSync)
    └── chunks/
        ├── 00/
        │   └── 1f/<full-32-hex-char filename>   content-addressed chunk
        └── ...
```

Directories are per-volume rather than per-cache-root because:

- Wiping one volume's cache (e.g. after a stale-data incident) is a simple
  directory removal.
- The LOCK file scope is naturally per-volume — two mounts of different
  volumes on the same host do not conflict.
- Each volume gets its own bbolt file; write contention is bounded by the
  number of concurrent accessors to that single volume.

Chunk path layout: `chunks/<first-2-hex>/<next-2-hex>/<rest>`. This produces
65,536 leaf directories and keeps any single directory under a few thousand
files for typical cache sizes.

### 6.2 bbolt schema (`meta.db`)

| Bucket | Key | Value |
|---|---|---|
| `meta` | `format_version` | uvarint, currently `1` |
| `meta` | `created_at` | unix nanos (big-endian uint64) |
| `attr` | path bytes | gob `persistedAttr{Attr, Version, ExpiresAt, Negative}` |
| `dir` | path bytes | gob `persistedDir{Entries []DirEntry, ExpiresAt time.Time}` |
| `data_idx` | `path\x00uvarint(chunkIndex)` | gob `persistedChunkRef{Hash [16]byte, Size uint32, Version uint64}` |
| `chunk_refs` | hash bytes (16) | uvarint refcount |
| `lru` | uvarint counter | bucket-qualified key (reserved; not yet actively written) |
| `lru_pos` | bucket-qualified key | uvarint counter (reserved; not yet actively written) |

`persistedAttr.Version` and `persistedChunkRef.Version` store the
`Attr.version` token so revalidation after a restart can use `GetAttrIfChanged`
instead of a full `Stat`. The `Version` field was provisioned in the initial
persistence implementation (as zero) and is populated with real values once
the server's `VersionFromAttr` machinery is active.

**Serialization:** gob. Standard library, compact, no external dependency.
Struct evolution is handled by the `format_version` wipe-on-mismatch policy
(see §6.4) rather than field migrations, which is consistent with the project's
no-backwards-compatibility stance.

**`lru` / `lru_pos` buckets** are provisioned in the schema for future access-
time-ordered eviction. They are not yet actively written; current disk
eviction is FIFO by lexical key order (see §3).

### 6.3 Content-addressed chunk storage

Chunks are stored under their `xxh3-128(chunk_bytes)` hash (library:
`github.com/zeebo/xxh3`, pure-Go with AVX-512/NEON paths). The 128-bit
output is the hex filename under the two-level shard tree.

Properties that follow from content addressing:

- **Deduplication:** a chunk whose bytes already exist on disk is not written
  again — the index entry simply references the existing file. Rename and copy
  operations are free in the cache (only index entries change; chunk files stay
  put).
- **Atomic writes:** chunk files are written via a temp-file-then-rename
  (`chunks/.tmp-<rand>` → final path). A chunk file at its final path either
  has its full bytes or does not exist.
- **Refcounting:** the `chunk_refs` bucket tracks how many `data_idx` entries
  point at each hash. A chunk file is only unlinked when its refcount reaches
  zero. Refcount updates happen in the same bbolt transaction as the index
  change, so the index and refcount are always consistent from bbolt's
  perspective.

xxh3-128 throughput is ~30 GB/s single-threaded (~35 µs per 1 MiB chunk).
It is not cryptographic; the local disk cache is trusted.

### 6.4 `format_version` and wipe-on-mismatch

On `persist.Open`, the code checks `meta.db`'s stored `format_version`
against the compiled-in constant (currently `1`). On mismatch:

1. Drop all buckets in `meta.db` and recreate the schema at the current
   version.
2. Remove the entire `chunks/` tree and recreate the directory.

No migration code. Release notes document the wipe when the version bumps.

### 6.5 LOCK file and single-process ownership

`persist.Open` acquires an exclusive non-blocking `flock(LOCK_EX | LOCK_NB)`
on `<cache.path>/<volume>/LOCK`. The lock is held until `persist.Close()` or
process exit (the kernel releases `flock` locks on fd close).

On `EWOULDBLOCK`, `Open` retries on a 100 ms poll interval for up to
`DefaultLockAcquireTimeout = 5 s` before returning `ErrCacheLocked`. The
retry window is sized to survive the unmount-then-remount race: when a user
does `Ctrl-C` on a running mount, the prior process issues several
`fusermount3` retries (some of which may fail on a busy FUSE mount) plus a
lazy-unmount fallback before it can call `persist.Close()` and drop the flock.
A re-mount issued immediately after the Ctrl-C lands inside this window; the
5 s budget absorbs the latency. `ErrCacheLocked` is only returned after the
budget is exhausted, at which point it surfaces as a clear "another gMountie
mount is using this cache directory" message.

The `LockAcquireTimeout` option can be set negative to disable the retry
(single attempt), which is useful in tests that assert fast-fail semantics.

## 7. Crash safety and startup reconciliation

### 7.1 Crash consistency invariants

The cache is fully reconstructable from the server; user data is never held
exclusively in the cache. There is therefore no durability requirement stronger
than "best-effort" for the cache index. On an unclean crash bbolt rolls back
to the last synced state (a consistent older snapshot) and the following
consequences are possible:

| What was lost | Consequence | Resolution |
|---|---|---|
| A read-fill index entry (chunk arrived, index not yet flushed) | Cache miss on next access | Re-fetch from server (transparent) |
| An index entry whose chunk file was never written | Ghost entry: index exists, chunk file absent | `data.go` loader: treat as cache miss + clean up inline; also sampled ghost sweep at startup |
| A chunk file whose index entry was lost | Orphan chunk: file exists, no index reference | Background orphan sweep unlinks it |
| A real invalidation that was not flushed | Stale chunk survives; served from cache | See §8.3 for the real-invalidation sync that prevents this |

### 7.2 Startup reconciliation

After acquiring the LOCK and validating `format_version`, `persist.Open`
performs three reconciliation steps:

1. **Budget enforcement.** Sum `data_idx` value sizes; if the total exceeds
   `disk_max_bytes`, run FIFO eviction until under budget. This catches the
   case where the user lowered the budget in config between restarts.

2. **Sampled ghost sweep.** Sample 1% of `data_idx` entries; for each, `stat`
   the referenced chunk file. If missing, delete the index entry and decrement
   the refcount (queuing a chunk unlink when refcount hits zero). The 1%
   sample catches the common case (a single recent crash). The remaining 99%
   are handled lazily: if a `dataCache.get` finds an index entry but the chunk
   file is absent, it treats the result as a cache miss and cleans up the index
   entry inline.

3. **Background orphan sweep.** A goroutine walks the entire `chunks/` tree
   after Open returns; for each file, it checks `chunk_refs[hash] > 0`. If not
   referenced, the file is unlinked. The cache is usable during this sweep.
   Files newer than 60 seconds are skipped to guard against a race where a
   chunk file has been written but its `IncRef` has not yet been committed to
   the bbolt index.

Both sweeps are conservative: they only delete things that are demonstrably not
referenced. Over-aggression yields cache misses; under-aggression yields orphan
disk usage — both are tolerable.

### 7.3 Validity state on restart

When `cache.subscribe_enabled = true`, a freshly-opened `cachedBackend` starts
in `stateUnverified`. Reads gate on `GetAttrIfChanged` until the Subscribe
stream delivers its first HEARTBEAT. This means a reboot scenario costs one
small RPC per accessed path before that path can be served from disk — but only
one (subsequent reads are verified).

When `cache.subscribe_enabled = false`, the backend is marked globally verified
at construction. Combined with the real-invalidation sync described in §8.3,
this remains safe: any invalidation that was flushed before the prior shutdown
is durable in bbolt; any invalidation that was not flushed results in a
potentially stale cache entry, but the real-invalidation sync ensures that any
entry deleted by an invalidation op was fsynced synchronously.

## 8. Durability tradeoffs: the NoSync optimization

### 8.1 Background

With the persistent cache enabled, every FUSE `Write` triggers two bbolt
transactions in the naïve implementation:

1. `InvalidateChunkRange` → `db.Update` over the written range.
2. `attr.invalidate` → `DeleteAttrBytes` → `db.Update`.

On a freshly written path (the common write-bench case), both transactions find
nothing to delete — they are no-ops. bbolt commits an `fdatasync` on every
writable transaction regardless, including empty ones. On a VM with a slow
fsync path (not uncommon on cloud block storage), this cut sequential write throughput to roughly a third of the cache-disabled rate — a regression attributable entirely to empty cache syncs.

### 8.2 No-op invalidation skip

Before opening a writable bbolt transaction, each invalidation function first
runs a read-only `db.View` scan over the target key range. If no keys are
present, the function returns immediately without opening an `Update`. This
is behavior-preserving: a no-op transaction deletes nothing whether or not
it commits.

Functions affected: `InvalidateChunkRange`, `InvalidatePathChunks` (in
`persist/dataidx.go`), `DeleteAttrBytes` and its dir-cache equivalent (in
`persist/kv.go`).

The cost of the read-only probe is a `db.View` cursor walk — on a path that
was never cached, the bucket will be empty and the walk terminates
immediately. This is microseconds, versus the milliseconds an `fdatasync` can cost on slow storage.

### 8.3 `NoSync` meta.db with periodic sync and real-invalidation sync

`meta.db` is opened with `bolt.Options{NoSync: true}`. This removes
per-commit `fdatasync` from all writable transactions, including the real
ones (cache populate, chunk index insert). Commits update the mmap in memory
and are flushed by:

- **Periodic sync:** a background goroutine calls `db.Sync()` every 1 second
  (`metaSyncInterval`). This bounds the loss window after an unclean crash to
  ~1 s of recently-populated entries.
- **Final sync at Close:** `persist.Close()` calls `db.Sync()` before
  `db.Close()`. This ensures that a clean shutdown (normal `Ctrl-C`, graceful
  unmount) leaves meta.db fully durable.
- **Sync after real invalidations (required correctness fallback):** when
  `subscribe_enabled = false`, `NewCachedBackend` immediately marks the
  validity tracker globally verified at construction. This bypasses the
  per-path `GetAttrIfChanged` revalidation gate entirely: a stale chunk that
  survived an unclean crash (because its invalidation was not fsynced) would
  be served as a trusted cache hit with no server round-trip to detect the
  staleness. To close this gap, each invalidation path in `persist/dataidx.go`
  and `persist/kv.go` calls `db.Sync()` immediately after a `db.Update` that
  actually deleted something. The no-op probe (§8.2) guarantees that `Update`
  only runs when at least one entry was present, so every successful `Update`
  is a real removal — the sync call fires on and only on real invalidations.
  This is not a belt-and-suspenders measure; it is the mechanism that makes
  subscribe-off mode correct under crash.

The net result is: the hot write path (invalidating a range that was never
cached) pays zero fsyncs. The read-fill path (populating the cache on first
read) pays zero per-entry fsyncs — the periodic sync flushes the index in the
background. Only real invalidations (a cached entry being overwritten) pay a
synchronous fsync. On the common sequential-write-bench workload (fresh file,
nothing cached), the cache write penalty is effectively eliminated.

`NoSync` is unconditional rather than behind a config flag. The cache is a
reconstructable index; per-commit durability is not required for correctness.
This is a behavior change from previous versions; release notes document it.

## 9. Configuration

### 9.1 Client config keys (`cache.*`)

All keys are under the `cache` prefix. Env vars use `GMOUNTIE_CACHE_*`.

| Key | Default | Description |
|---|---|---|
| `cache.enabled` | `true` | Insert the cache decorator at mount time. `false` disables the cache entirely. |
| `cache.subscribe_enabled` | `true` | Open a Subscribe stream for push invalidation. `false` = TTL-only mode; cache starts globally verified. |
| `cache.path` | `$XDG_CACHE_HOME/gmountie` | Base directory for per-volume cache dirs. Each volume gets a subdirectory. |
| `cache.memory_max_bytes` | `268435456` (256 MiB) | In-memory tier byte cap across all sub-caches. `0` = unbounded. |
| `cache.disk_max_bytes` | `10737418240` (10 GiB) | On-disk `chunks/` byte cap. `0` = unbounded. |
| `cache.chunk_size_bytes` | `1048576` (1 MiB) | Data cache chunk granularity. Range: `[4096, 16777216]`. |
| `cache.attr_ttl` | `5m` | Positive attr cache TTL. `0` = no expiry. |
| `cache.dir_ttl` | `5m` | Dir listing cache TTL. `0` = no expiry. |
| `cache.negative_ttl` | `30s` | Negative (ENOENT) attr cache TTL. `0` = no expiry. |
| `cache.xattr_ttl` | `5m` | XAttr names cache TTL per entry. `0` = no expiry. |

### 9.2 Server config keys (`server.*`)

| Key | Default | Description |
|---|---|---|
| `server.subscribe_buffer_size` | `256` | Per-subscriber event channel buffer. A slow subscriber that fills this buffer is dropped. |
| `server.subscribe_heartbeat_interval` | `10s` | How often the server sends a HEARTBEAT to all volume subscribers. Also serves as the keepalive for the stream. |

## 10. What this intentionally does not do

- **Write-back caching.** Writes go through to the server immediately. The
  cache only holds data populated on reads. Dirty data is never held
  exclusively in the client cache.

- **Encrypted cache at rest.** The local disk cache is trusted. Encryption
  of the `meta.db` and `chunks/` tree is deferred; it would be purely an
  at-rest protection and adds complexity to the key management surface.

- **Cross-process shared cache.** The LOCK file enforces single-process
  ownership. Two mounts of the same volume from the same machine must use
  separate `cache.path` directories (or they will conflict at the lock step).
  A shared multi-process cache tier is a future phase.

- **Per-range invalidation.** Subscribe events carry `(path, new_version)`.
  The server's Write handler also knows the `(offset, length)` of the
  mutation, but this is not yet surfaced in the protocol. Per-chunk version
  tracking and range-selective invalidation would save round-trips on
  large-file, tiny-write workloads. Deferred until profiling shows this is
  a hot path.

- **Out-of-band change detection (inotify/fanotify).** The event bus is
  self-emit only. A hybrid inotify watcher per volume root (enabled via a
  future `server.subscribe_watch_external` flag) would catch direct edits to
  the underlying directory. Until then, TTL is the safety net for out-of-band
  changes.

- **Subscribe event replay ring.** For short disconnects the server could
  replay recent events on reconnect, removing the need for per-path
  `GetAttrIfChanged` calls during reconnection. Deferred until profiling
  shows the revalidation cost matters.

- **Access-time LRU eviction on disk.** The `lru`/`lru_pos` bbolt buckets
  are provisioned in the schema and the eviction loop exists; however,
  access-time tracking is not yet wired. Current disk eviction is FIFO
  by lexical key order.

- **`SetXAttr` / `RemoveXAttr` in the cache.** These operations are not yet
  on `FileSystemBackend`. When they are added, they will need invalidation
  entries in the table in §5. Note: xattr *names* (via `ListXAttr`) are now
  cached as of §2.3; it is xattr *write* operations that remain unimplemented.

- **XAttr value caching.** `GetXAttr` values are large and content-dependent;
  only the names list from `ListXAttr` is cached (§2.3). Value caching is
  deferred until profiling shows it is a hot path.

## 11. Glossary

- **Version** — a uint64 freshness token packed from `(mtime_ns, size, ctime_ns)` by `VersionFromAttr` in `pkg/server/io/version.go`. Carried in `Attr.version` (proto field 18). Zero means "no version known" (older server or uninitialized cache entry).

- **Verified / Unverified** — the two states of `validityTracker`. Verified: the Subscribe stream is healthy and has delivered at least one HEARTBEAT since stream-up; cached reads are trusted within TTL. Unverified: Subscribe is down or was never enabled after a restart; every cached read goes through a `GetAttrIfChanged` revalidation before being served.

- **Chunk** — a fixed-size slice of a file's content, stored under its xxh3-128 hash in the `chunks/` tree. Default size 1 MiB. The unit of data cache storage and invalidation.

- **Ghost entry** — a `data_idx` bbolt entry that points at a chunk file that does not exist on disk. Arises from a crash between committing the index entry and completing the chunk file write. The loader detects and cleans these up inline; the startup sampled ghost sweep catches them in bulk.

- **Orphan chunk** — a chunk file in `chunks/` with no corresponding `chunk_refs` entry (or a zero refcount). Arises from a crash after a chunk file was written but before its refcount was committed, or after a refcount-zero commit but before the unlink syscall. The background orphan sweep removes these at startup.

- **`format_version`** — a uvarint stored in `meta.db`'s `meta` bucket. Mismatch between the running code's compiled constant and the stored value triggers a full cache wipe (meta.db + chunks/) and reinitialisation at the current version.

- **LOCK file** — `<cache.path>/<volume>/LOCK`. An `flock(LOCK_EX | LOCK_NB)` on this file enforces single-process ownership of the per-volume cache directory. Released on `persist.Close()` or process exit.

- **NoSync** — a bbolt database option that skips the per-commit `fdatasync`. meta.db is opened NoSync unconditionally; a periodic 1 s background sync and a final sync at Close bound the loss window. Real invalidation transactions perform an immediate `db.Sync()` as a safety measure (see §8.3).

- **Subscribe** — `RpcFs.Subscribe(volume)`, a server-streaming RPC that pushes `SubscribeEvent` messages to connected clients whenever a mutation occurs on the named volume. The primary freshness mechanism; TTL is the fallback.

- **HEARTBEAT** — a `SubscribeEvent` with `Kind = HEARTBEAT` sent by the server on a periodic ticker (default every 10 s). Has no path or payload; its role is to signal "the stream is alive and you have received all events up to this point." The client flips the validity tracker to Verified after the first HEARTBEAT on a new stream.
