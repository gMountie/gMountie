# Client Architecture — Composable Backend Layer Stack

**Status:** Shipped (PR worktree-rwo-wal, 2026-06-22)
**Last updated:** 2026-06-22

This document describes the durable architecture of the gMountie client's
backend layer stack as built. It covers the package layout, the two composition
planes, the layering contracts, capability negotiation, the metrics seam, and
the conformance harness. The RWO-lease and WAL design that motivated this work
is recorded separately in [rwo-wal.md](rwo-wal.md) (paused).

---

## 1. Package layout

```
pkg/client/
  backend/              — FileSystemBackend interface, PassthroughBackend base,
  │                       FileHandle + Unwrap contract, shared types (Attr, FileLock…)
  │  backend.go         — interface + handle contract
  │  passthrough.go     — PassthroughBackend embeddable base
  │  passthrough_test.go — TestFileSystemBackendMethodSet pin guard
  │
  ├─ transport/         — gRPC leaf: BackendClient, retryOp, WriteCoalescer drive
  ├─ cache/             — cachedBackend decorator (two-tier memory+disk)
  ├─ observer/          — metricsLayer observer decorator
  ├─ identity/          — IDRewriter (UID/GID rewrite for squash mode)
  ├─ memfs/             — in-memory reference backend (testing)
  └─ contract/          — RunBackendContract conformance harness

  fuse/
  ├─ gofuse/            — go-fuse v2 fs.Node* adapters (Linux)
  └─ cgofs/             — cgofuse adapters (macOS/FUSE-T/macFUSE)

  mount/
    params.go           — MountParams, negotiateMountParams
    single.go           — SingleVolumeMounter: stack assembly + FUSE mount
```

The presentation split is one-way: `fuse/{gofuse,cgofs}` consume
`backend.FileSystemBackend`; they have no knowledge of which layers are in the
stack below them.

---

## 2. The two composition planes

### 2.1 Backend plane (cross-cutting)

`backend.FileSystemBackend` is the op-shaped decorator seam. Every layer
implements this interface and wraps an `inner FileSystemBackend`. The stack
assembled at mount time is:

```
fuse/{gofuse,cgofs}  (presentation — one-way consumer)
        ↓
[observer layers]    (e.g. metricsLayer)
        ↓
cache                (cachedBackend — semantic)
        ↓
transport            (BackendClient — gRPC leaf, owns Subscribe)
```

Observer layers sit above the cache so they see the boundary as the FUSE
adapter does. The cache is a semantic layer between observers and the transport.

Stack assembly lives in `pkg/client/mount/single.go` as named, ordered
positions — not a free-form list and not an `if`-ladder. Adding a layer means
inserting it at the appropriate named position.

### 2.2 Handle plane (per-fd)

`backend.FileHandle` is its own decorator chain with an `Unwrap()` contract.
The transport's `resolveHandle` walks `Unwrap()` to reach the concrete
`*grpcFileHandle` at the leaf. Any layer that introduces per-fd state (dirty
flags, coalescer buffers, future WAL fd-state) must wrap the handle *and*
participate in the Unwrap walk so the transport can still resolve its fd.

```go
type FileHandle interface {
    Unwrap() FileHandle  // leaf returns itself
}
```

Per-fd state lives on the handle chain, not on the backend layer. A design
expressed only in terms of backend decorators cannot express write-batching.

---

## 3. Two layer contracts: observer vs. semantic

**Observer layers** are pure pass-through with side effects. They embed
`backend.PassthroughBackend` and override only the ops they observe. A future
method is silently forwarded — correct for an observer (the op just goes
through untouched).

```go
type metricsLayer struct {
    backend.PassthroughBackend  // all unobserved ops forward transparently
    rec metrics.Recorder
}
```

**Semantic layers** change behavior (cache, future writeBatcher/WAL). They also
embed `PassthroughBackend` and override only the ops they handle, **but** the
`TestFileSystemBackendMethodSet` pin guard (see §5) ensures that adding a new
method to the interface breaks the build until every semantic layer is reviewed.
A silent forward through a cache or WAL layer is a stale-data or durability
bug; the compiler must catch it.

---

## 4. Four load-bearing invariants

1. **Per-fd state lives on the handle chain, and layers join the Unwrap walk.**
   `resolveHandle` walks `Unwrap()` until it reaches `*grpcFileHandle`; opaque
   intermediate handle wrappers are transparent to it.

2. **Retry lives only in the transport leaf.** `retryOp` in `transport/` is the
   single retry point. No layer above the transport adds its own retry loop.
   Layered retries multiply deadlines and produce nested retry storms.

3. **Invalidation propagates up.** The transport owns the single `Subscribe`
   subscription to the server's event stream. Invalidation events flow **up**
   the chain (transport → cache → node); no layer reaches around the chain to
   the raw client. This is the coherence path.

4. **Observer layers: embed and override. Semantic layers: embed and pin.**
   Both embed `PassthroughBackend`; the difference is the method-set pin guard
   (§5) that makes the semantic-layer contract compiler-enforced.

---

## 5. Capability negotiation — MountParams

`mount.MountParams` is resolved **once, before the backend stack is built**,
from the existing RPCs (`Version.Get` → `MaxWriteBytes`; `WhoAmI` →
`MappingMode`, `DefaultPermissions`). It is then passed to each layer's
constructor and to `establishMount`. This eliminates the positional-arg sprawl
and the "backend built before negotiation" ordering hazard.

```go
// pkg/client/mount/params.go
type MountParams struct {
    MaxWriteBytes      uint32
    DefaultPermissions bool
    // extended on future WhoAmI additions
}
```

No new wire surface is introduced: `MountParams` is assembled from fields that
already exist on `Version.GetReply` and `WhoAmIReply`.

---

## 6. Metrics seam — metrics.Recorder

`pkg/client/metrics.Recorder` is a leaf-package interface (no deps on io or
grpc) injected per-client into the observer layer, the cache layer, and the
transport retry path. The Prometheus implementation (`*metrics.Metrics`) is the
default; OTel and audit backends implement `Recorder` without touching any
layer. This replaces the previous package-global dispatcher
(`RegisterInstance`/`UnregisterInstance`, global `instances` slice) that existed
only to dodge an import cycle — the cycle is gone because layers now depend on
the `Recorder` interface, not on the concrete metrics package.

**Boundary vs. internal signals.** The observer layer (`backend/observer/`)
emits boundary signals visible from outside (per-op latency, count, error code).
Layer-internal signals (cache tier hit/miss, persist counters, subscribe-stream
state) stay emitted from within their layers through the same injected
`Recorder`. Not "one metrics layer for everything" — boundary signals belong to
the observer; internal signals belong home.

---

## 7. Conformance harness

`pkg/client/backend/contract.RunBackendContract` is a reusable conformance
suite run against every `FileSystemBackend` implementation: `memfs` (the
in-memory reference backend), `transport` (gRPC leaf), `cache`, `observer`, and
`identity`. It asserts close-to-open consistency and the behavioral contract
documented on the interface: write/flush/release ordering, error propagation,
idempotency expectations, and retry ownership. `pkg/client/backend/contract_test.go`
wires every layer into the suite so a new layer's conformance is verified by
dropping it in.

**Method-set pin guard.** `TestFileSystemBackendMethodSet` in
`pkg/client/backend/passthrough_test.go` pins the exact method set of
`FileSystemBackend` using reflection. Adding, removing, or renaming a method
fails here, forcing a deliberate review of every embedding semantic layer before
the change lands. The guard is the compiler-enforced half of invariant #4.

---

## 8. FileSystemBackend behavioral contract (summary)

The full contract is documented on the interface in `pkg/client/backend/backend.go`.
Key points:

- **Write/Flush/Release ordering.** A `Flush` drains the write coalescer and
  flushes the server-side buffer atomically in one RPC (`WriteAndFlush`
  fusion). `Release` follows `Flush`; a layer must not reorder these.
- **Retry ownership.** Only the transport leaf retries. Layers propagate errors
  upward without re-attempting.
- **Idempotency.** Mutating ops carry a `request_id`; the server deduplicates
  within a session window. Layers must not strip or reuse `request_id`s.
- **Handle Unwrap.** A layer that wraps a handle returns the inner handle from
  `Unwrap()`; a leaf returns itself. `resolveHandle` depends on this contract.
- **Invalidation direction.** Invalidation events flow up (transport →
  upper layers → node). No layer reaches around the chain to the raw
  subscription client.
- **Concurrency safety.** `FileSystemBackend` methods may be called concurrently
  from multiple goroutines. Each layer is responsible for its own
  synchronization.

---

## 9. Deferred future work

Three seams are visible in the design but intentionally not built now:

- **Per-fd handle-layer seam for write-path layers** (`writeBatcher`, WAL).
  The `WriteCoalescer` drive logic is intentionally welded to the transport
  because it fuses coalesce-drain and flush into one RTT (`WriteAndFlush`).
  Lifting it into a composable layer severs that fusion and adds a new
  inter-layer durability contract through the most delicate part of the write
  path, for no behavioral gain until a *second* write-path layer (the WAL)
  shares the seam. The WAL slot is reserved; the seam lands with the WAL,
  perf-gated against Bencher.

- **Invalidation routing up through the chain.** The transport owns `Subscribe`
  and today delivers events directly to the cache. Routing events up through
  the full chain (so any layer can observe or transform them) is deferred until
  there is a consumer above the cache that needs it.

- **Optional stack-assembly / FUSE-mounting split.** `SingleVolumeMounter`
  currently assembles the stack and mounts in one step. Separating them (so a
  caller can assemble a stack without mounting) is a future extensibility point,
  not needed today.
