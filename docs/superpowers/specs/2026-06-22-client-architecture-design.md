# Client architecture — clean, extensible layering (design)

**Date:** 2026-06-22
**Branch:** `worktree-rwo-wal`
**Status:** design. Scope is **client-first internal extensibility**. The WAL and
the RWO lease ([`2026-06-22-rwo-wal-design.md`](2026-06-22-rwo-wal-design.md)) are
**paused** — this design *accommodates* them (their slots are visible) but does
not implement them. Server extensibility is explicitly **not** built here (see
§Server).

---

## Goal

Make the gMountie **client** a clean, composable stack of layers so new
cross-cutting behavior plugs in by *adding a layer*, not editing the hot path.
Evolve the decorator seam that already exists — do **not** rewrite.

Non-goals: extending the server for cloud now; building the WAL/lease; any new
wire surface.

## Where we are (and the three welds)

The client already has good bones: `node.go → FileSystemBackend → cachedBackend →
transport` is a real decorator seam, and the go-fuse adapters delegate cleanly
through `FileSystemBackend` (`pkg/client/io/backend.go:110`). Three welds stop it
being genuinely extensible:

1. **Composition is a hardcoded `if`-ladder** in the mount factory
   (`pkg/client/mount/single.go:118-135`) — adding a layer edits the factory body.
2. **Write-batching is welded into the transport leaf** — `WriteCoalescer` lives
   on `grpcFileHandle` and is driven inline by `Write/Flush/Fsync/Release`
   (`backend_grpc.go:1551`, `:1031-1092`), so batching is not a composable concern.
3. **The cache subscribes to invalidation *around* the interface** (the raw
   transport client, `single.go:134`, `cache/subscriber.go:41`), so any layer
   below the cache is invisible to invalidation.

Plus a control-path wart: capabilities are negotiated *after* the backend is
built and threaded as scattered positional args
(`single.go:118` builds the backend before `WhoAmI` at `:145`; `maxWrite` /
`defaultPermissions` thread through `dispatch_{linux,darwin,cgofuse}.go`).

## Target architecture

```
            ┌─ invalidation events flow UP ─┐
            ▼                                │
node → [observer layers] → cache → [writeBatcher] → transport (dumb leaf)
        (metrics/trace)            └ WAL/lease slot ┘   owns server subscription
```

Two composition planes, both already present, both generalized:

- **Backend plane** — layers implement `FileSystemBackend` (the op surface) and
  wrap an `inner FileSystemBackend`. This is the cross-cutting seam.
- **Handle plane (per-fd)** — `FileHandle` is its own decorator chain with an
  `Unwrap()` contract (`backend.go:90-102`); `resolveHandle` walks it to reach the
  transport handle (`backend_grpc.go:1601`). **Per-fd state (coalescer buffers,
  dirty flags, future WAL fd-state) lives here, not on the backend.** Any layer
  needing per-fd state must wrap the handle *and* participate in the Unwrap walk,
  or the transport layer can't resolve its fd. A design that only describes
  backend decorators cannot express write-batching — this plane is the real
  data-path seam.

### Three invariants (load-bearing — get these wrong and it's unbuildable later)

1. **Per-fd state lives on the handle chain, and layers join the Unwrap walk.**
   `resolveHandle` currently hard-asserts `*grpcFileHandle`; the target is that it
   walks `Unwrap()` until it reaches the transport handle, so intermediate per-fd
   layers are transparent to it.
2. **Two kinds of layer, two contracts:**
   - **Observer layers** (metrics, tracing, audit) — pure pass-through + side
     effects. They embed a `PassthroughBackend` base and override only the few ops
     they observe. Safe: a future 31st method is silently forwarded, which is
     correct for an observer.
   - **Semantic layers** (cache, future writeBatcher/WAL) — they change behavior.
     They **implement the full `FileSystemBackend` surface explicitly** (no
     passthrough embed), so adding a method *breaks their build* until someone
     consciously decides how that op interacts with the layer. Silent forwarding
     of a new mutating op through a cache/WAL is a stale-data bug; the compiler
     must catch it.
3. **Invalidation has a direction: up.** The **transport** owns the single server
   `Subscribe` subscription. Events propagate **up** the chain (transport → … →
   cache → node); each layer may observe or transform them. No layer reaches
   around the chain to the raw client. (This is the cache-coherence path — the
   async-persist stale Heisenbug lives here — which is exactly why the routing
   change is deferred until a consumer can validate it; see #4.)

### Capability flow

Resolve one `MountParams` value **before** building the stack, from the
**existing** RPCs only (`Version.Get` → `frame_size_bytes`; `WhoAmI` →
`mapping_mode`), then pass it to each layer's constructor and to `establishMount`.
This kills the positional-arg sprawl and the "backend built before negotiation"
ordering bug. **No new wire surface** (see §Server guard).

## The five moves — execution split

| # | Move | Plane | Status |
|---|------|-------|--------|
| 1 | Composition list replaces the `if`-ladder (`single.go`) | backend | **NOW** |
| 2 | Canonical layer order `cache → [writeBatcher] → transport` | backend | **NOW** (declares the order; writeBatcher slot is empty until #3) |
| 5 | `MountParams` resolved pre-build from existing RPCs, threaded as one value | control | **NOW** |
| — | `PassthroughBackend` base (observer layers only) | backend | **NOW** |
| 3 | Lift `WriteCoalescer` out of the transport leaf into a `writeBatcher` handle-layer | handle | **DESIGN-ONLY / DEFER** |
| 4 | Route the invalidation stream up through the chain | backend | **DESIGN-ONLY / DEFER** |

**Why #3 and #4 defer.** Each has exactly one consumer: a *semantic write-path*
layer — the WAL (and the lease) — which is paused. #3 is hot-path with real perf
risk; #4 is in the coherence-sensitive path. Executing either now is
destabilization risk with **no consumer to validate against** — the "build on
spec" trap. They stay in this design (so the writeBatcher slot and the WAL
position are visible and the now-work doesn't paint them into a corner) and land
**with the feature that needs them**, perf-gated (#3) against Bencher.

**#3 is specifically NOT a cleanliness win today (it's a mild regression).**
Verified against the code 2026-06-22:

- The batching *algorithm* is already cleanly extracted — `WriteCoalescer`
  (`pkg/client/io/coalesce.go`) is a self-contained, internally-locked unit with
  a tight `Append`/`Drain` surface. The part that benefits from isolation is
  already isolated; there is no tangled blob to clean up.
- What's interleaved into `grpcFileHandle` is the *drive* logic, and it is fused
  with transport concerns **on purpose**:
  - **`WriteAndFlush` fusion** (`backend_grpc.go:1149`): `Flush` collapses
    drain-coalescer + flush into a *single* RPC — one RTT instead of two — only
    because coalescing and flushing are co-located on the transport handle.
    Lifting the batcher into a layer above the transport splits these into two
    ops across a boundary and **loses the fusion** unless it is re-plumbed across
    the seam.
  - **Optimistic-return + sticky write-back error** (`recordWriteErr`/
    `takeWriteErr`, `backend_grpc.go:1058`/`:1119`): "ack the write to FUSE, then
    if the deferred send fails, stick the error so a later Flush/Release surfaces
    it." That contract lives exactly at the coalesce↔transport boundary.
  - Plus the `dirty`-flag fast path, `request_id`/idempotency interplay, and
    `reclaimIfStale`/retry — all transport-specific.
- So extracting now adds a new inter-layer drain/durability/error contract
  through the most delicate part of the write path, for **identical behavior**,
  while risking the one-RTT fusion — more surface, not less. It becomes a genuine
  cleanliness *and* correctness win only when a **second** write-path layer (the
  WAL) shares the seam, at which point "two buffers welded in two places" → "one
  composable write-path slot with a defined contract" pays for itself.
- (If the motive were ever just to slim the overloaded `grpcFileHandle`,
  read-path prefetch/readahead is a cleaner first extraction candidate than the
  transport-fused coalescer.)

**Honest scope of the NOW subset.** #1/#2/#5 + the passthrough base make the stack
extensible for **observer layers** (metrics, tracing, audit) and fix the
capability-ordering wart. They do **not** by themselves enable **semantic
write-path layers** — that's what #3/#4 unlock. The now-work is the clean
foundation and the honest 80%; it does not claim full extensibility.

## Server (light — not extended now)

Per direction: don't build server extension now, just **don't regress** the
future path (the `server.New(cfg, ...Option)` direction recorded in
[[project_oss_extensibility_seam]] / the cloud's lapsed-but-sound proposal).
Concretely, the only obligations on *this* (client) work:

- **No new wire surface.** Build `MountParams` from `Version` + `WhoAmI` as they
  exist. A generic mount-time negotiation primitive is a *future RWO-mode* need;
  introducing it now would be speculative server/wire surface that cuts **against**
  the don't-regress guard, not for it. When RWO returns, that's when a negotiation
  primitive earns its place — designed generically so a future extended server
  benefits.
- Keep any shared changes (proto, config idioms) additive and generic, never
  client-only hacks that a future server seam would have to work around.

The server's own internal-extensibility cleanup is **deferred** and tracked
separately; it is not on the critical path for the client.

## Out of scope / deferred

- WAL, RWO lease, the explicit durability-contract method (WAL-specific).
- Server `server.New(...Option)` + `With*` seams (future; not built now).
- Any new RPC, proto field, or capability-negotiation primitive.
- A typed op-pipeline / per-concern interface split — rejected: unidiomatic for a
  ~30-method syscall surface and a needless rewrite; the decorator model is right.

## Open questions for review

1. Order #2: `cache → writeBatcher → transport` matches today's behavior
   (write-through cache, then coalesce). Confirm that's the intended read/write
   mental model.
2. The NOW subset (#1/#2/#5 + passthrough) — is "observer-layer extensibility now,
   semantic-layer seams with the feature" the right line, or do you want #3
   pulled forward despite having no consumer yet?
