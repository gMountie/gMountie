# Compound-for-writes (SP3): fuse the close-tail Write+Flush

**Date:** 2026-05-25
**Status:** approved design — re-scoped to a focused `WriteAndFlush` after an
empirical packet trace (see "Measured baseline") showed RELEASE is already
async and only Write+Flush is collapsible. The general `MutatingCompound`
explored earlier was over-built for the measured win.
**Branch:** `worktree-proto-v2-compound-writes` (off `origin/master` `40b652e`)
**Scope:** wire protocol (`api/proto/file.proto`), server controller
(`pkg/server/controller/file.go`), client io (`pkg/client/io/backend_grpc.go`).

## Context

**SP3** of a decomposed "proto v2" effort. A `tc netem` RTT study found the
FUSE→gRPC protocol severely WAN-RTT-bound and ranked the pieces by measured
leverage. **The agreed move after SP3 is SP4 (bidi-stream `Open`)**, which would
subsume general compound/streaming machinery — so SP3 is deliberately kept
minimal: the smallest change that lands the measured small-file write win,
nothing built speculatively for a compound future that SP4 replaces.

Other proto-v2 pieces (SP1 metadata-to-headers, SP2 `google.rpc.Status` —
dropped, SP5 delete `Keepalive`) are out of scope; each gets its own spec.

## Problem (measured)

Small-file create-write-close is the dominant WAN cost. netem bench (kubevirt
VM, loopback, cache enabled), 150× `echo x > f`:

| RTT | wall | files/s |
|---|---|---|
| ~0 | 6.5s | 23.1 |
| ~50ms | 64.7s | 2.3 |
| ~100ms | 126.6s | 1.2 |

≈ 8 RTTs/file, linear in RTT. Per-RPC count for one `echo x > f` (server
metrics, `grpc_code="OK"` deltas): `GetAttr`×2, `GetXAttr`×1, `Create`×1,
`Write`×1, `Flush`×2, `Release`×1.

**Packet trace (`tcpdump` on `lo`, 100ms RTT, 5 files):** 4.0s total ≈ **~7
critical-path round-trips per file** (36 ~100ms-spaced gaps over 5 files;
sub-ms packet clusters are HTTP/2 framing within a single RPC). **RELEASE shows
no distinct critical-path gap → it is issued async by the kernel and does not
block `close()`.** This is the decisive finding: the only RPCs worth fusing on
the close path are **`Write` + `Flush`**.

## Goals

1. Fuse the deferred coalesced write and the flush into **one** RPC
   (`WriteAndFlush`) at FUSE FLUSH time — the point whose errno reaches the
   application's `close()`.
2. Eliminate the post-create `GetAttr` by returning the new file's `Attr` in the
   `Create` reply, and the post-flush attr refresh by returning the final `Attr`
   in the `WriteAndFlush` reply.
3. No regression to large streamed writes; RELEASE path unchanged (already async).
4. Minimal surface: this is the increment before SP4, not a compound framework.

Target: small-file create-write-close drops from ~7 critical-path RTTs to ~5
(fuse Write+Flush saves ~1–2; `Create`-returns-`Attr` saves ~1), measured on the
netem W1 workload. The remaining ~3 (`GetAttr` lookup, `GetXAttr` probe) are the
negative-caching follow-on, out of scope.

## Non-goals

- **General `MutatingCompound`** (ordered op list, current-fd register,
  abort-on-error contract, B1-shaping). Over-built for fusing two RPCs;
  superseded by SP4. Explicitly dropped.
- **B1 (speculative deferred `Open`)** — folding `Create` into the batch.
- **Same-file/cross-file metadata batching.** Same-file metadata mostly arrives
  *post-close on the path* (tar's `chmod`/`chown`/`utime`), which serializes and
  needs speculation; `git checkout` puts mode in `open()`. Out of scope.
- **RELEASE changes** — already async/off the critical path.
- **Negative-caching** the `GetAttr` lookup + `GetXAttr` ENODATA probe (the other
  ~3 RTTs). Cache-layer follow-on.
- No backwards compatibility (project controls both ends; documented in release
  notes).

## Design

### 1. `WriteAndFlush` RPC (wire)

A new unary RPC on `RpcFile`. Unary, not streaming: it is only used when the
pending coalesced buffer is small (≤ `WriteCoalesceBytes`, default 1 MiB, well
under the 16 MiB message cap). Large files stream via the existing `Write` and
this RPC carries only the small tail + the flush.

```proto
message WriteAndFlushRequest {
  string volume     = 1;   // connection-scoped fields stay until SP1 moves them
  uint64 fd         = 2;
  int64  offset     = 3;
  bytes  data       = 4;   // the drained coalescer buffer; may be empty (pure flush)
  string session_id = 5;
}

message WriteAndFlushReply {
  uint32 written    = 1;
  int32  status     = 2;   // FUSE errno: write error if the write failed,
                           // else the flush errno (0 = OK)
  Attr   final_attr = 3;   // file attrs after write+flush (refreshes the inode)
}
```

Added to the service: `rpc WriteAndFlush (WriteAndFlushRequest) returns (WriteAndFlushReply);`

### 2. `Create` returns `Attr`

`CreateReply` gains `Attr attributes = 3;`. The controller fills it from the
stat it already performs post-create (`versionAfterPath` path), so no extra
server-side RTT. The client uses it to populate the kernel `EntryOut`,
eliminating the client→server post-create `GetAttr`.

### 3. Server handler (`pkg/server/controller/file.go`)

`WriteAndFlush`: resolve session + fd (as `Write`/`Flush` do), then **write then
flush in order**:
- if `len(data) > 0`, `entry.File.Write(data, offset)`; on non-OK, return that
  errno in `status` and **do not flush** (write error is what `close()` should
  see);
- then `entry.File.Flush()`; return its errno in `status`;
- stat the path and populate `final_attr` (reuse the `GetAttr`→`proto.Attr`
  mapping already in `fs.go`).
- emit the mutation event on the bus (as `Write`/`Flush` paths do) so subscribers
  invalidate.

`Create`: after a successful `fs.Create`, populate `reply.Attributes` from the
post-create stat it already takes for `versionAfterPath`.

### 4. Client (`pkg/client/io/backend_grpc.go`)

`BackendClient.Flush` changes from *(drain coalescer → streaming `Write`) + `Flush`
RPC* (2 RTTs) to **one `WriteAndFlush`** (1 RTT):
- `Drain()` the coalescer in-memory (no RPC);
- call `WriteAndFlush(fd, drained.Offset, drained.Data)` (empty `data` when the
  drain is nil — a pure flush);
- apply `final_attr` to the handle/inode; return `fuse.Status(reply.Status)`.
- **No-op skip:** if the coalescer is empty *and* nothing has been written since
  the last flush (a clean handle), skip the RPC and return OK — covers the
  second of the two FLUSHes the kernel sometimes issues.

`Create` consumes `reply.Attributes` to fill `EntryOut` without a follow-up
`GetAttr`.

`Release` and `Fsync` are unchanged. (`Fsync` keeps its own drain+`Fsync` because
it is a mid-stream durability boundary, not the close tail.) Large writes still
stream via `Write` on coalescer overflow exactly as today; `WriteAndFlush` then
carries only the residual tail.

## Failure model

- **Errors land where `close()` reads them.** `WriteAndFlush` runs at FUSE FLUSH,
  whose errno is returned to `close()`. Write-before-flush ordering means a write
  failure is reported as the `close()` errno and the flush is skipped — identical
  observable behavior to today's separate `Write`-then-`Flush`, in one round trip.
- **No fd-lifecycle change.** The client holds the fd (eager `Create`) exactly as
  today; `WriteAndFlush` references it; `Release` still closes it. No
  current-fd register, no orphaned-fd hazard.
- **Idempotency.** `WriteAndFlush` is wrapped in the existing `retryableCall`
  like `Flush`/`Fsync`: a re-run re-writes the same bytes at the same offset and
  re-flushes — idempotent for the overwrite-at-offset semantics the coalescer
  already relies on.

## Testing

- **Server (testify suites, `pkg/server/controller`):**
  - `WriteAndFlush` writes the data at offset then flushes; reply `written` and
    `final_attr` correct; bus event emitted.
  - write failure → `status` = write errno, flush NOT performed.
  - empty `data` → pure flush, OK, `final_attr` populated.
  - `Create` reply carries `Attributes`.
- **Client (`pkg/client/io`):**
  - `Flush` issues exactly one `WriteAndFlush` carrying the drained buffer (mock
    asserts no separate streaming `Write` + no separate `Flush` RPC).
  - clean-handle `Flush` (empty coalescer, nothing written since last flush)
    issues no RPC.
  - large write still streams via `Write` on overflow; `WriteAndFlush` carries
    the tail.
  - `Create` fills `EntryOut` from `reply.Attributes` with no `GetAttr` call.
  - `final_attr` is applied to the inode.
- **Mocks:** regenerate (`task gen:mocks`) for the new RPC.
- **E2E / VM (blocking acceptance):** re-run the netem W1 workload (150×
  `echo x > f`) at ~50ms and ~100ms RTT. **Target: ~7 → ~5 critical-path
  RTTs/file** (verify via per-RPC server metrics: `Write` and `Flush` counts per
  file drop, `WriteAndFlush` = 1; `GetAttr` drops by the post-create one). W2
  (128 MiB streamed write) throughput unchanged. **Prefer the `test/e2e/perf`
  harness** (TCP+netem+cache, `task perf:bench:tcp`) if it has merged to `master`
  by implementation time; otherwise the ad-hoc netem bench.

## Acceptance criteria

1. A small-file create-write-close issues **one** `WriteAndFlush` for the close
   tail; the separate streaming `Write` + `Flush` RPCs are gone for that path
   (server per-RPC metrics).
2. `Create` reply carries `Attr`; the post-create `GetAttr` round-trip is gone.
3. Clean-handle flushes issue no RPC; large streamed writes (W2) unregressed;
   `Fsync` and `Release` behavior unchanged.
4. Write-error-at-close surfaces correctly (write errno returned, flush skipped).
5. VM netem re-bench: W1 critical-path RTTs/file drop from ~7 toward ~5.
6. `task lint` and `task test` pass.

## Files expected to change

- `api/proto/file.proto` — `WriteAndFlush` RPC + request/reply messages;
  `CreateReply.attributes`. Regenerate stubs (`task gen:grpc`).
- `pkg/server/controller/file.go` — `WriteAndFlush` handler; populate
  `CreateReply.Attributes`.
- `pkg/client/io/backend_grpc.go` — `Flush` uses `WriteAndFlush` + no-op skip;
  `Create` consumes `Attributes`.
- `internal/mocks` — regenerate (`task gen:mocks`).
- Release notes — proto change (no BC).
