# Cache fsync reduction (no-op invalidation skip + NoSync meta.db)

**Date:** 2026-05-24
**Status:** approved design, pending spec review → implementation plan
**Scope:** client-side persistent cache (`pkg/client/cache`, `pkg/client/cache/persist`)

## Problem

A 1 MiB sequential-write bench on the kubevirt VM (`local-path` PVC) capped at
~31.6 MiB/s with the persistent cache enabled, vs ~101 MiB/s with the cache
disabled — a 70 MiB/s gap. Profiling attributed the loss to
`syscall.Fdatasync` under bbolt commits on the client write path.

Investigation corrected the original mental model:

- The cache is **not** write-back. `cachedBackend.Write` writes through to the
  **server** first (`inner.Write`, whose status is returned to the kernel),
  then *invalidates* the local cache range. Cached chunks are only populated on
  **read** (`Read` → `data.put`). There is never dirty user data held only in
  the cache.
- On the bench (a fresh file, nothing cached yet) each FUSE `Write` fires two
  bbolt commits that **delete nothing**:
  1. `InvalidateChunkRange` → `db.Update` over a range with no entries.
  2. `attr.invalidate` → `DeleteAttrBytes` → `db.Update` (no-op after the first
     write to the path).

bbolt commits + `fdatasync`s on every writable transaction **even when the
transaction dirtied nothing**. A microbenchmark on the dev host confirmed the
structure (numbers are host-specific; the host has fast fsync, the VM did not):

| Bench | ns/op |
|---|---|
| `EmptyUpdate_Sync` (no-op commit, fsync on) | 34,376 |
| `EmptyUpdate_NoSync` (no-op commit, fsync off) | 23,847 |
| `InvalidateChunkRange_FreshPath` (the real call) | 35,209 |

`InvalidateChunkRange` on a never-written path costs the same as an empty
commit, and the synced variant pays for an fdatasync that protects nothing.

## Goals

1. Eliminate fsyncs for invalidation transactions that delete nothing (the
   dominant cost on the write bench). **No durability or correctness change.**
2. Remove per-commit fsync from the cache index generally, since the index is
   fully reconstructable. **Best-effort durability with a bounded loss window.**
3. Preserve: no user-data loss, no stale reads served after a crash.

## Non-goals

- Async writeback queue (was "Option C"). Not justified by current data; revisit
  only if A+B leave a gap.
- Any change to the server, the wire protocol, or write-through semantics.
- New config keys. Sync cadence is a hardcoded constant until evidence demands a
  knob (matches the `lockRetryInterval` precedent and the project's
  "config only when needed" convention).

## Design

### A — Skip no-op invalidation commits

Probe with a read-only transaction (`db.View`, no fsync, ~µs) before opening a
writable transaction; only `db.Update` when there is something to delete.

- `Persist.InvalidateChunkRange(path, firstIdx, lastIdx)` — `View`-scan the
  range keys; if none are present, return `nil` without opening an `Update`.
- `Persist.InvalidatePathChunks(path)` — `View` cursor over the path prefix;
  if the prefix has no keys, return without `Update`.
- `Persist.DeleteAttrBytes(key)` and the dir-cache remover's persist delete —
  `View`-`Get` the key; if absent, return without `Update`.

A no-op transaction deletes nothing whether or not it commits, so skipping it is
behavior-preserving. This removes the per-write fsyncs on the fresh-file write
path entirely.

### B — `NoSync` meta.db + periodic Sync

The cache index (`meta.db`) is reconstructable: losing a recent entry yields a
cache miss and a re-fetch, never data loss. Durability is therefore best-effort
by design.

- Open `meta.db` with `bolt.Options{ ..., NoSync: true }` (or set `db.NoSync =
  true` post-open). Commits update the mmap and skip the per-commit fdatasync.
- A background goroutine calls `db.Sync()` on a fixed interval — **1s**,
  expressed as a package constant (e.g. `metaSyncInterval`) — to bound the loss
  window. `Close()` performs a final `db.Sync()` before `db.Close()`.
- Lifecycle integrates with the existing `startBackgroundSweeps` start path and
  the corresponding shutdown so the syncer goroutine is stopped before
  `db.Close()`.

This speeds the read-fill `PutChunkRef`/`PutAttrBytes` path (real mutations that
A cannot help) and any real invalidations.

### Crash consistency

No user-data loss under any crash: writes are durable on the server before
`Write` returns; the cache never holds the only copy. On an unclean crash bbolt
rolls back to the last synced meta — a *consistent* older state — and:

| Lost on crash | Consequence | Mitigation |
|---|---|---|
| Read-fill index entry | cache miss | re-fetch from server (existing) |
| Index ref whose chunk file is gone | `ReadChunk` errors | loader returns `hit=false` → cache miss (existing, `data.go` loader) |
| Chunk file with no index ref (orphan) | wasted disk | orphan sweep unlinks it (`sweep.go`, `runOrphanSweep`) |
| An invalidation | stale chunk survives in cache | on restart the backend opens in `stateUnverified` and revalidates against the server before serving cached bytes (`validity.go`) |

### The stale-invalidation dependency (explicit decision)

The last row depends on the validity/subscribe layer resetting to
`stateUnverified` on a fresh backend and gating reads until revalidation.
**Decision:** rely on the validity layer and document the dependency.

**Implementation-time verification result: UNSAFE → fallback TAKEN.**

Verification traced the read-path gates (`backend.go:142, 188, 231, 283`) —
all correctly skip cached bytes when `globalState() != stateVerified`. The
startup default of `newValidityTracker()` is `stateUnverified` (zero value,
`validity.go:16,27`). However, `backend.go:52-57` contains a branch that fires
unconditionally when `cfg.SubscribeEnabled == false`:

```go
if !cfg.SubscribeEnabled {
    b.validity.markGlobalVerified()   // ← immediately trusts the cache
}
```

This means that with subscribe disabled (a supported operator config), a
freshly-opened `cachedBackend` starts in `stateVerified`, bypassing all
per-path revalidation. A stale chunk that survived a crash (because its
invalidation was not fsynced) would be served as a trusted cache hit.

**Fallback implemented:** `db.Sync()` is called immediately after the writable
transaction in each real-invalidation path:
- `persist/dataidx.go` — `InvalidatePathChunks` and `InvalidateChunkRange`,
  after `p.db.Update(...)` returns nil (the no-op probes above guarantee Update
  only runs when ≥1 entry is present, so every successful Update is a real
  removal).
- `persist/kv.go` — `kvDelete`, after `p.db.Update(...)` returns nil (the
  presence probe above guarantees the same).

These sync calls are only reached when an entry was actually deleted; the hot
write path (invalidating a range that was never cached) is unaffected.
A test `TestRealInvalidationDurableAfterReopen` in `persist/fsync_test.go`
confirms the deleted entries are absent after a close+reopen cycle.

### `NoSync` is unconditional (explicit decision)

`meta.db` is opened `NoSync` unconditionally rather than behind a config flag.
Justification: it is a cache; durability is not required for correctness (the
write-through-to-server invariant plus the crash table above). This is a
behavior change from per-commit durability, documented here and in release
notes. No backwards-compatibility concern (single project controls both ends).

## Testing

- **Unit (testify suites):**
  - No-op `InvalidateChunkRange` / `InvalidatePathChunks` / `DeleteAttrBytes`
    open no writable transaction (assert via a commit/txn counter or
    `db.Stats().TxStats.Write` delta).
  - A real invalidation still deletes the entry and decrements the chunk
    refcount correctly (no regression in existing `dataidx`/`refcount` tests).
  - The periodic syncer fires and `Close()` performs a final sync (assert via
    `db.Stats()` sync counters or a synthetic crash-replay).
  - Loader degrades to a miss (`hit=false`) when the referenced chunk file is
    absent.
- **Regression benchmark:** a proper `Benchmark` in `pkg/client/cache/persist`
  for the no-op invalidation path (replacing the throwaway used during
  investigation).
- **VM re-bench (blocking acceptance):** repeat the 1 MiB sequential-write fio
  bench on the kubevirt VM's `local-path` PVC and record the before/after
  MiB/s. The dev host's fast fsync cannot demonstrate the win; the VM is the
  reference environment. Target: close the bulk of the 31.6 → ~101 MiB/s gap.

## Files expected to change

- `pkg/client/cache/persist/dataidx.go` — `InvalidateChunkRange`,
  `InvalidatePathChunks` View-probe.
- `pkg/client/cache/persist/kv.go` (or wherever `DeleteAttrBytes` lives) —
  View-probe before delete.
- `pkg/client/cache/persist/persist.go` — `NoSync` on open, periodic syncer
  goroutine, final sync in `Close`, lifecycle wiring.
- Tests alongside each, plus a persist-package benchmark.
- Release notes — document unconditional `NoSync` behavior change.

## Acceptance criteria

1. No-op invalidations open no writable transaction (unit-proven).
2. `meta.db` runs `NoSync` with a 1s periodic sync and a final sync on `Close`.
3. Crash-consistency behavior matches the table above; the stale-invalidation
   dependency is either satisfied by the validity layer (verified) or covered by
   the sync-after-real-invalidation fallback.
4. VM re-bench shows a material write-throughput improvement over 31.6 MiB/s.
5. `task lint` and `task test` pass.
