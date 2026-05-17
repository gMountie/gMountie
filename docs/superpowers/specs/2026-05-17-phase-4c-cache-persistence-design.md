# Phase 4 / Sub-spec C: Cache Persistence

**Status:** Design approved 2026-05-17.

**Builds on:**
- Sub-spec A (`docs/superpowers/specs/2026-05-17-phase-4a-pathfs-to-fs-migration.md`) — FileSystemBackend decorator seam.
- Sub-spec B (`docs/superpowers/specs/2026-05-17-phase-4b-in-memory-cache-ttl.md`) — three in-memory caches (attr, dir, data), `cachedBackend` decorator, accountant/store/LRU primitives.

**Parallels:** Sub-spec D (Subscribe + version push) — independent and can land in either order.

## Goal

Persist the client-side cache to disk so it survives `gMountie mount` restarts. After the first read of a file, the bytes don't cross the network again — even after stopping and restarting the client. Three caches persist: attribute, directory, data chunks. Negatives stay memory-only.

## Non-goals

- Subscribe push / `Attr.version` server-side population (Sub-spec D).
- Cross-process shared cache (Phase 8).
- Encrypted cache at rest (deferred per roadmap; trust local disk).
- Write-back caching (still write-through, same as B).

## Architecture

Sub-spec C is a backing-store change to Sub-spec B's stores, plus a new persistence package. The `cachedBackend` decorator, the `attrCache`/`dirCache`/`dataCache` types, the read-through/write-through policy, and the FileSystemBackend chain all stay. Two pieces change:

1. **New `pkg/client/cache/persist/` package.** Owns the bbolt database, the chunks/ directory, xxh3-128 chunk hashing, the lock file, format versioning, startup reconciliation, and a `Store` interface that the in-memory stores compose over. Does not know what an `attrEntry` is — works in bytes.

2. **Memory tier above disk.** The existing memory `store` becomes a hot tier in front of `persist.Store`. `get` checks memory → falls through to disk → on disk hit, promotes to memory; `put` writes through to both. The Sub-spec B `accountant` continues to own the *memory* LRU and budget. A parallel `diskAccountant` owns the disk LRU and budget. The two are independent (see "Caps" below).

`cachedBackend` in `backend.go` is unchanged. Same Read/Write/Stat/Rename methods, same invalidate calls — persistence is invisible to it.

### Package boundaries

| Package | Responsibility |
|---|---|
| `cache/persist` | bbolt schema, chunks/ layout, xxh3-128 hashing, atomic chunk writes via tmp+rename, refcount-based chunk unlinks, lock file, format_version, startup reconciliation. Untyped — `(bucket, key, value []byte)` + chunk file references. |
| `cache/attr.go`, `dir.go`, `data.go` | Typed serialization (gob) into/out of persist. Define their bucket names. Read-through + write-through composition. |
| `cache/backend.go` | Unchanged. Policy (when to invalidate, when to populate, TTL clocks). |
| `cache/store.go` | Extended: memory tier with fall-through to a `persist.Store`. Existing `accountant` integration unchanged. |

## Caps (two independent budgets)

Disk legitimately scales to TB on a workstation; memory has hard ceilings. A unified cap forces operators to pick one or the other. Two independent caps:

- **`cache.memory_max_bytes`** — default 256 MiB. Caps the in-memory tier across all three cache types. Existing `accountant` owns it. (Replaces Sub-spec B's `max_size_bytes`.)
- **`cache.disk_max_bytes`** — default 10 GiB. Caps `chunks/` byte count plus bbolt file approx. New `diskAccountant` owns it.

Interactions:

- **Memory evict.** Entry stays on disk (it was written through). Subsequent gets hit disk and (optionally) re-promote.
- **Disk evict.** Corresponding memory entry is dropped (disk is the source of truth; can't have a memory entry pointing at a missing chunk). On the same hash, this is a no-op for entries the memory tier didn't hold.

Memory and disk eviction are LRU within their tier. There is no cross-tier promotion penalty — promoting a disk hit into memory may push out a colder memory entry; that entry stays on disk.

## On-disk layout

Per-volume directories under `cache.path`:

```
<cache.path>/
└── <volume-name>/
    ├── LOCK              advisory flock; refuses startup if held
    ├── meta.db           bbolt database
    └── chunks/
        ├── 00/
        │   └── 1f/<full-xxh3-128-hex>   content-addressable chunk file
        └── ...
```

Per-volume (not global with volume keys) because:

- Easier to wipe one volume's cache without touching another.
- Lock file scope is naturally per-volume — two mounts of *different* volumes don't conflict.
- Each volume gets its own bbolt file; contention is bounded.

Chunk path: `chunks/<first-2-hex>/<next-2-hex>/<rest>` — 65,536 leaf directories, keeps any single directory under a few thousand files for the cache sizes we target.

## Chunk addressing

Content-addressable: chunk file path is determined by `xxh3-128(chunk_bytes)`. Rename and copy of a file are essentially free in the cache (only the bbolt index entries change; chunk files stay put and pick up new references). Deduplication across files comes for free.

Hashing happens on every cache populate (read-through completion). xxh3 throughput on commodity hardware is ~30 GB/s single-threaded; for a 1 MiB chunk that's ~35 µs — effectively free on the read-through path. 128-bit output gives ~10^19 chunks before birthday-bound collision risk becomes interesting, plenty for a single-user cache.

We're not using the hash for cryptographic strength — local disk is trusted, so we pick raw speed over crypto-grade. Library: `github.com/zeebo/xxh3` (pure-Go xxh3, AVX-512/NEON paths).

## bbolt schema (`meta.db`)

| Bucket | Key | Value |
|---|---|---|
| `meta` | `format_version` | uvarint, currently `1` |
| `meta` | `created_at` | unix nanos |
| `attr` | path bytes | gob `persistedAttr{Attr, Version, ExpiresAt, Negative}` |
| `dir` | path bytes | gob `persistedDir{Entries []DirEntry, ExpiresAt time.Time}` |
| `data_idx` | `path\x00uvarint(chunk_index)` | gob `persistedChunkRef{Hash [16]byte, Size uint32, Version uint64}` |
| `chunk_refs` | hash bytes | uvarint refcount |
| `lru` | uvarint counter | bucket-qualified key (e.g. `data_idx/...`) |
| `lru_pos` | bucket-qualified key | uvarint counter (reverse index for O(1) promote/remove) |

`Version` is the per-roadmap `Attr.version` that Sub-spec D will populate; we lay the field now (zero = "no version") so D ships without a bbolt migration.

**Serialization choice:** gob. Std-lib, smaller than JSON, simpler than proto. Brittle to struct evolution — but the wipe-on-mismatch `format_version` policy makes evolution painless: bump the version, structs change, old cache gets wiped on next open.

**format_version mismatch policy:** wipe `meta.db` + `chunks/` and recreate. No migrations; per the project's no-BC stance. Release notes document the wipe.

## Read path (data chunk)

1. `dataCache.get(path, idx)` checks the memory store.
2. **Memory hit** → existing `accountant.touch`, return bytes. No disk I/O.
3. **Memory miss** → check `data_idx` bucket for `path\x00uvarint(idx)`.
   - **Disk hit** → read `chunks/aa/bb/<hash>`, copy into a new `[]byte`, insert into memory tier (may trigger memory eviction), bump disk LRU (batched — see below), return bytes.
   - **Disk miss** → return nil; `cachedBackend.Read` falls through to inner backend.
4. After backend fetch, `dataCache.put` writes through to both tiers (see Write).

Attr and dir reads follow the same shape with simpler value handling.

## Write path (cache populate, not user write)

1. Compute `hash := xxh3.Hash128(chunk)` (16 bytes).
2. If `chunks/aa/bb/<hash>` already exists → dedupe; skip the bytes write.
3. Otherwise: write `chunks/aa/bb/.tmp-<rand>`, atomic-rename into final path. No fsync (best-effort durability — cache is reconstructible from the server).
4. Single bbolt txn: insert `data_idx[path\x00idx] = {hash, size, version}`, increment `chunk_refs[hash]`, append LRU entry. If this push exceeds `disk_max_bytes`, the same txn evicts the oldest entries (drops index entries, decrements their refcounts, queues now-zero-ref chunk files for unlink which happens post-txn).
5. Insert into memory tier (may trigger memory eviction).

## Invalidation

Same contract as Sub-spec B (`dataCache.invalidatePath` / `invalidateRange`, `attrCache.invalidate`, `dirCache.invalidate`). Now also hits the persist layer:

- Memory side: existing `removeMatching` / `remove` scan unchanged.
- Disk side: bbolt cursor over the relevant bucket with prefix, delete each + decrement refcount for chunks. Unlink chunk files post-commit when refcount hits zero. Refcount updates live in the same txn as the index changes, so consistency is atomic from the index's perspective.

## Refcount-based chunk lifecycle

Dedupe means one chunk file may be referenced by multiple `(path, idx)` entries. The `chunks/` file can only be unlinked when its refcount hits zero.

- Insert: `refs[hash]++` in the same txn as the index insert.
- Evict / invalidate: `refs[hash]--`; if zero, queue unlink (post-txn).
- Orphan reconciliation on startup cleans up any unlinks lost to a crash mid-cleanup.

## LRU batching

Per-hit bbolt writes would serialize all reads through the single writer. Memory-resident LRU position (already exists in `accountant`) stays authoritative for in-memory ordering. bbolt's `lru` / `lru_pos` buckets store the *persisted* order — written only at:

- Insert (new entry into disk tier).
- Eviction.
- Periodic flush every 30 s of recently-touched entries.
- Clean shutdown.

On crash, recently-touched entries fall back to their last-persisted order — some LRU accuracy lost, substantial throughput gained. Same tradeoff rclone makes.

## Concurrency model

- bbolt: single-writer, many-reader. All writes go through one `*Persist` goroutine queue. Reads (cursor walks for prefix invalidation) are sync.
- Chunk file I/O: lock-free (atomic rename + ref-counted unlinks).
- Lock file: advisory `flock(LOCK_EX | LOCK_NB)` on `<cache.path>/<volume>/LOCK` — only one process at a time owns this cache dir.
- Memory tier's existing `store.mu` + `accountant.mu` lock order from Sub-spec B is unchanged.

## Lock file

On startup, `persist.Open(path, volume)` acquires `flock(LOCK_EX | LOCK_NB)` on `<cache.path>/<volume>/LOCK`. On failure (`EWOULDBLOCK`), return a typed error `ErrCacheLocked` that mount-wiring surfaces as a clear "another gMountie mount is using this cache directory" message. Lock is released on `Close()` or process exit (kernel handles `flock` release).

## Crash safety

Invariants we maintain:

- bbolt has its own WAL; index updates are atomic per-txn.
- Chunk files are written via tmp-rename, so a chunk file at `chunks/aa/bb/<hash>` either has its full bytes or doesn't exist.
- The only inconsistency window is between (a) bbolt index commit and (b) the post-commit chunk file write/unlink.

Two failure modes:

1. **Orphan chunk file** — file exists on disk, no index entry. Happens if we wrote a chunk but crashed before committing the index txn, or after a refcount-zero unlink commit but before the unlink syscall.
2. **Ghost index entry** — index entry exists, chunk file missing. Happens if we committed an index entry pointing at a chunk that hadn't finished its tmp-rename yet.

Both are recoverable; neither corrupts user data. Worst case is a cache miss.

## Startup reconciliation

After `flock` succeeds and `format_version` is validated:

1. **format_version check** — if mismatch, wipe `meta.db` + `chunks/`, recreate at the current version.
2. **Validate disk budget** — sum `data_idx` value sizes (bbolt stats); if over `disk_max_bytes`, run LRU eviction until under. Required by roadmap DoD.
3. **Ghost sweep (sampled + lazy)** — sample 1% of `data_idx` entries; for each, `stat(chunks/...)`. If missing, delete the index entry + decrement refcount. The remaining 99% are handled lazily during normal operation: if a `dataCache.get` finds an index entry but the chunk file is missing, treat as cache miss and clean up the index entry inline. The sampled-upfront pass catches the common case (a single recent crash) without blocking startup on a multi-million-entry walk.
4. **Orphan sweep** — walk `chunks/` tree async after Open; for each file, check `chunk_refs[hash]` exists and > 0. If not, unlink. Cache is usable during this sweep.

Steps 3 and 4 are conservative: never delete user data (chunks are always re-fetchable from the server), so over-aggression is acceptable.

## Config

`pkg/client/config/cache.go` extends:

```yaml
cache:
  enabled: true              # NEW DEFAULT (was false in B). Sub-spec C ships default-on.
  path: ~/.cache/gmountie    # NEW. Default via adrg/xdg.
  memory_max_bytes: 268435456  # NEW. 256 MiB. Replaces max_size_bytes.
  disk_max_bytes:  10737418240 # NEW. 10 GiB.
  chunk_size_bytes: 1048576  # unchanged
  attr_ttl:      5s          # unchanged
  dir_ttl:       5s          # unchanged
  negative_ttl:  2s          # unchanged
```

**`max_size_bytes` → `memory_max_bytes` + `disk_max_bytes`.** The old key is removed, not aliased. Sub-spec B was default-off, so the break only affects operators who explicitly opted in. Release notes document.

**Default-on flip.** Roadmap calls for `cache.enabled: true` once stable. Sub-spec C is "stable" by definition of done. Flipping in this sub-spec ships persistence to anyone who runs `gMountie mount` with default config.

## Metrics

Extend the Phase 2 cache metrics to split memory and disk:

- `gmountie_cache_hits_total{tier="memory|disk", type="attr|dir|data"}`
- `gmountie_cache_misses_total{type="attr|dir|data"}` (miss = neither tier had it)
- `gmountie_cache_evictions_total{tier="memory|disk", type="..."}`
- `gmountie_cache_bytes{tier="memory|disk"}`
- `gmountie_cache_dedupe_hits_total` (chunk file already existed on populate)

## Testing strategy

**Unit (testify suites):**

`cache/persist`:
- bbolt schema CRUD round-trips
- lock file contention (second `Open` errors with `ErrCacheLocked`)
- chunk refcount lifecycle (insert, dedupe, invalidate, unlink only at zero)
- eviction under `disk_max_bytes`
- format_version mismatch triggers wipe
- ghost + orphan sweep correctness (inject inconsistency, verify cleanup)

`cache` (extended Sub-spec B tests):
- Parameterize over `persist=nil` (memory-only) and `persist=tempDir`.
- Memory→disk fallthrough and write-through.

**E2E (`test/e2e/api/cache_test.go`):**

`CachePersistentFSSuite`:
- Write/read files to populate cache.
- Close client.
- Re-open client with same `cache.path`.
- Re-read same files; assert backend was NOT invoked for cached chunks (Phase 2 metrics or a request-count interceptor).
- Assert dual-mount fails with `ErrCacheLocked`.

**Eviction-under-cap e2e** (roadmap DoD):
- `disk_max_bytes = 100 MiB`, read 1 GiB of distinct files; verify cap holds and most-recent files served from disk.

## Definition of done

- A read of a file already in the cache hits the network only for attr revalidation. (Zero-RPC reads are Sub-spec D.)
- The cache survives a `gMountie mount` restart with the same `cache.path`.
- Pointing two mounts at the same cache path fails fast with `ErrCacheLocked`.
- E2E: `disk_max_bytes = 100 MiB` + 1 GiB of distinct file reads → cap holds, recent files served from disk.
- Cache hit/eviction/bytes metrics split by tier (memory/disk).
- `task test` passes including the new `CachePersistentFSSuite` and persist-package unit suites.

## Out of scope (explicit)

- Server-side `Attr.version` population — Sub-spec D.
- `Subscribe(volume)` server-streaming RPC — Sub-spec D.
- Cross-process shared cache — Phase 8.
- Encrypted cache at rest — deferred.
- Migration code between `format_version` values — wipe-on-mismatch instead.
- Write-back caching — same write-through as B.
