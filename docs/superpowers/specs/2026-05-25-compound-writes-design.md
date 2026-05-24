# Compound-for-writes: collapse create-write-close to minimal round-trips

**Date:** 2026-05-25
**Status:** approved design, pending spec review → implementation plan
**Branch:** `worktree-proto-v2-compound-writes` (off `origin/master` `40b652e`)
**Scope:** wire protocol (`api/proto`), server controllers (`pkg/server/controller`), client io (`pkg/client/io`)

## Context: where this sits

This is **SP3** of a larger, decomposed "proto v2" effort. A `tc netem` RTT
study (loopback, kubevirt VM, see "Measured baseline") showed the FUSE→gRPC
protocol is severely WAN-RTT-bound, and ranked the architect's 5-part plan by
measured leverage:

| | Piece | Status |
|---|---|---|
| SP1 | connection-scoped fields (`volume`/`session_id`/`request_id`/`Caller`) → gRPC metadata | future, foundational |
| **SP3** | **Compound-for-writes (this spec)** | **now — highest measured leverage** |
| SP4 | `Open` → bidi stream | future; W2 showed real but secondary value |
| SP2 | `int32` status → `google.rpc.Status` | dropped/opportunistic (fixes nothing measured) |
| SP5 | delete `SessionService.Keepalive` | future, trivial, standalone |

Only SP3 is in scope here. The others are recorded so the decomposition isn't
lost; each gets its own spec later.

## Problem (measured)

A small-file create-write-close is the dominant WAN cost. netem bench (1 MiB
loopback, cache enabled), 150× `echo x > f`:

| RTT | wall | files/s |
|---|---|---|
| ~0 (baseline) | 6.5s | 23.1 |
| ~50ms | 64.7s | 2.3 |
| ~100ms | 126.6s | 1.2 |

≈ **8 RTTs per file**, scaling linearly with RTT. Per-RPC breakdown for one
`echo x > f` (server metrics, `grpc_code="OK"` deltas):

| RPC | count | collapsible by this work? |
|---|---|---|
| `GetAttr` (negative lookup + post-create attr) | 2 | post-create one: yes (via Create-returns-Attr); lookup: no (follow-on) |
| `GetXAttr` (kernel `security.capability` probe) | 1 | no (follow-on: negative xattr cache) |
| `Create` | 1 | B1 only (deferred) |
| `Write` | 1 | yes |
| `Flush` | 2 | yes |
| `Release` | 1 | yes |

The compoundable close-tail (`Write`+`Flush`×2+`Release` = 4 RPCs) plus the
post-create `GetAttr` are what this spec removes.

## Goals

1. Collapse a small-file create-write-close's **close-tail** (`Write` + same-file
   metadata + `Flush` + `Release`) into a **single** Compound RPC.
2. Eliminate the **post-create `GetAttr`** by returning the new file's `Attr` in
   the `Create` reply.
3. Land the win without regressing large-file streaming writes (the W2 path).
4. Shape the wire + client handle abstraction so **B1 (speculative deferred
   open)** is a later increment, not a rewrite.

Target: small-file create-write-close drops from ~8 RTTs to ~4–5 (B2), measured
on the netem W1 workload.

## Non-goals

- **B1 (speculative deferred `Open`)** — deferring `Create` into the Compound.
  The wire format must *support* it; we do not *implement* it here.
- **Cross-file metadata batching** (`rm -rf`, mass `unlink`/`mkdir`). FUSE
  serializes these and apps expect synchronous results; needs speculative
  execution. Out of scope.
- **Negative caching** of the pre-create `GetAttr` lookup and the `GetXAttr`
  ENODATA probe — the remaining ~2–3 RTTs. Real, but cache-layer work, not
  protocol. Separate follow-on.
- SP1 / SP2 / SP4 / SP5.
- No backwards compatibility: the project controls both client and server; the
  proto change is a clean break, documented in release notes.

## Design

### 1. Mutating Compound (wire)

A new Compound carrying an **ordered list of mutating ops**, distinct from the
existing read-only `Compound` in `fs.proto` (whose "no abort on per-op error"
semantics are wrong for writes).

```proto
// MutatingOp is one write-side op in a MutatingCompound. Ops execute in order
// and reference the "current fd" register (see below).
message MutatingOp {
  oneof op {
    WriteOp    write    = 1;  // data + offset, against current/explicit fd
    FlushOp    flush    = 2;
    ReleaseOp  release  = 3;
    ChmodOp    chmod    = 4;
    ChownOp    chown    = 5;
    TruncateOp truncate = 6;
    AllocateOp allocate = 7;
    CreateOp   create   = 8;  // B1: sets current fd; unused by B2 (eager Create)
  }
}

message MutatingCompoundRequest {
  repeated MutatingOp ops = 1;
  uint64 fd = 2;            // the client-held fd for B2 (ops with fd=0 use it);
                            // 0 when the batch opens its own fd via a CreateOp (B1)
}

message MutatingOpResult {
  int32 status = 1;         // FUSE errno for this op (0 = OK)
  uint32 written = 2;       // WriteOp only
}

message MutatingCompoundReply {
  repeated MutatingOpResult results = 1;  // one per executed op, in order
  int32 aborted_at = 2;     // index of the first failed op, or -1 if all ran
  Attr final_attr = 3;      // file attrs after the batch (satisfies close getattr)
}
```

**Current-fd register.** The server keeps a per-batch "current fd". A `CreateOp`
(B1) sets it. For B2, the client passes the already-open fd in
`MutatingCompoundRequest.fd`; per-op `fd` fields default to it. This unifies B2
(explicit client-held fd) and B1 (Create-in-batch) with no wire change between
increments.

**Abort-on-first-error.** Ops execute in order; on the first non-OK status the
server stops, records `aborted_at`, and returns results for the ops that ran.
(Contrast the read-only `Compound`, which runs all ops regardless.)

**Service placement.** The lifecycle spans today's `RpcFile` (Write/Flush/
Release/Allocate/Create) and `RpcFs` (Chmod/Chown/Truncate). With no BC
constraint, `MutatingCompound` is a new method on **`RpcFile`** and carries its
own op messages (`WriteOp`/`ChmodOp`/… — thin structs, not the legacy
per-RPC request types, which still carry the soon-to-move connection-scoped
fields). The legacy unary RPCs remain for the fallback path.

### 2. Client close-tail batching (B2)

The client file handle (`pkg/client/io`, the `gMountieFile`/handle in
`backend_grpc.go`) gains a **batching mode**:

- `open`/`create` issues a real `Create` eagerly (synchronous error, today's
  path) and records the fd.
- During the open window, instead of issuing `Write`/`Chmod`/`Chown`/`Truncate`
  immediately, the handle **buffers** them (the existing write-coalesce buffer
  extended to also hold pending metadata ops).
- On `Release`, the handle emits **one** `MutatingCompound` against the held fd:
  `[WriteOp(coalesced data)?, <metadata ops in arrival order>, FlushOp, ReleaseOp]`,
  then maps `MutatingCompoundReply` back to FUSE statuses and applies
  `final_attr` to the inode.

**Fallback to today's per-op path** (batching disabled for this handle) when any
of these occur before close:
- `fsync` mid-stream (durability boundary — must flush now);
- buffered write reaches the existing write-coalesce flush boundary
  (`WriteCoalesceBytes`, default 1 MiB) — this is already the small-vs-large
  dividing line, so the handle streams via the legacy `Write` exactly as today;
- a read on the handle (`read`-back needs current server state);
- a second writer / `dup` on the fd.

On fallback the handle flushes whatever it buffered via the legacy
`Write`/metadata RPCs and continues normally. **Designed for B1:** the same
handle abstraction later defers the `Create` itself (buffer it as a `CreateOp`
at the head of the batch, set `MutatingCompoundRequest.fd=0`), with additional
fallback triggers (read-back / fstat before close).

### 3. `Create` returns `Attr`

`CreateReply` gains an `Attr attributes` field; the controller populates it from
the post-create `GetAttr` it already does server-side (one local stat, no extra
RTT). The client uses it to fill the kernel `EntryOut`, eliminating the
client→server post-create `GetAttr` round-trip.

## Failure model

- **B2 fd ownership is clean:** the client holds the fd (from eager `Create`),
  so on any Compound abort it cleans up via its normal `Release` of the held fd.
  No orphaned-fd hazard (that only arises in B1, where the server owns the
  in-batch fd and must close it on abort — designed for, not implemented).
- **Error surfacing:** abort-on-first-error; the first non-OK op's status is
  returned at `close()` (via `Flush`/`Release`'s FUSE return). POSIX permits
  `close()` to report deferred write errors, so this is conformant.
- **Partial application is observable and bounded:** `aborted_at` + per-op
  `results` tell the client exactly which ops ran. E.g. Write OK but Chmod
  failed → data is on the server, mode is not; the client returns the chmod
  error at close and the file reflects the partial state (same as if the ops had
  run unbatched and chmod failed).

## Testing

- **Unit (testify suites):**
  - Compound builder: a buffered write + metadata + flush + release produces the
    expected ordered `MutatingOp` list against the held fd.
  - Current-fd resolution: ops with `fd=0` resolve to `MutatingCompoundRequest.fd`.
  - Abort semantics: an injected mid-batch failure stops execution, sets
    `aborted_at`, returns partial results; server-side state matches.
  - Fallback triggers: `fsync` / oversize-buffer / read-back / dup each disable
    batching and fall through to the legacy per-op path correctly.
  - `Create` reply carries `Attr`; client fills `EntryOut` without a `GetAttr`.
- **Server controller tests:** `MutatingCompound` handler executes ops in order,
  honors the current-fd register, aborts correctly, returns `final_attr`.
- **E2E / VM (blocking acceptance):** re-run the netem W1 workload (150×
  `echo x > f`) at ~50ms and ~100ms RTT with the new client+server; record
  RTTs/file. **Target: ~8 → ~4–5.** Confirm W2 (128 MiB streamed write)
  throughput is unchanged (fallback path intact). Confirm `git checkout`-style
  (write + chmod) and `tar -x`-style (write + chmod + truncate) sequences batch.

## Acceptance criteria

1. A small-file create-write-close issues **one** `MutatingCompound` for the
   close-tail (verified by server per-RPC metrics: `Write`/`Flush`/`Release`
   counts drop to 0, `MutatingCompound` = 1 per file).
2. `Create` reply carries `Attr`; post-create `GetAttr` count drops to 0.
3. All fallback triggers correctly revert to the legacy per-op path; large
   streamed writes (W2) unregressed.
4. Abort-on-first-error contract holds; errors surface at `close()`; no
   orphaned server fds.
5. VM netem re-bench shows W1 RTTs/file materially reduced toward ~4–5.
6. `task lint` and `task test` pass; the wire/client abstraction admits B1 as an
   additive change (documented, not implemented).

## Files expected to change

- `api/proto/file.proto` — `MutatingCompound` RPC + op/result messages;
  `CreateReply.attributes`. Regenerate stubs (`task gen:grpc`).
- `pkg/server/controller/file.go` (+ a new `compound_write.go`) — handler,
  current-fd register, abort logic, `final_attr`; populate `CreateReply.Attr`.
- `pkg/client/io/backend_grpc.go`, `coalesce.go`, the handle/`gMountieFile` in
  `node.go` — buffering of metadata ops, close-tail Compound emission, fallback
  triggers, `final_attr` application, `Create` attr consumption.
- `internal/mocks` — regenerate (`task gen:mocks`) for the new RPC.
- Tests alongside each; netem E2E notes in `testing/` (scratch).
- Release notes — proto break (no BC).
