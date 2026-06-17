# Client Connection Pool — Design

**Status:** Approved (brainstorm 2026-06-18)
**Goal:** Open N gRPC connections per mount (sharing one session) and round-robin Read/Write streams across them, so read and write throughput can exceed the single-connection TCP single-flow ceiling (~48 MiB/s observed) on high-BDP (1Gbit/WAN) links.

---

## Problem

A gRPC `ClientConn` multiplexes all streams over one HTTP/2 connection = one TCP flow. The killpriv/readahead/cache work bottomed out against that one flow: at 1Gbit, single-file write (~16 MiB/s) and the aggregate read/write ceiling (~48 MiB/s) are capped by the single connection, not by the link, the disk, or the server. More TCP flows are the lever to fill a high-BDP link.

## Key enabler (verified)

The server resolves a session by the `session_id` in request metadata and binds ownership to the **principal (cert CN), not the TCP connection** (`resolveSession`, `pkg/server/controller/idempotency.go`); the fd table is **per-session** (`pkg/server/service/session.go`). Therefore N client connections that all carry the same `session_id` (and the same principal) resolve the same session, and an fd opened on any connection is usable on all of them. No server change is required.

## Architecture

- The client opens **N** `grpc.ClientConn` to the same endpoint, where N = `rpc.connections` (default 4). All share **one session**.
- **Connection 0 is the primary.** It carries: the session handshake (`Connect`), the keepalive stream, the `Subscribe` stream, and ALL metadata RPCs (`Fs()`, `File()` Open/Create/Release/locks, `Volume()`, `Version()`).
- **Read and Write streams round-robin across all N connections, per stream.** The spread is per-stream (not per-handle) so a single large file's concurrent streams spread across flows: the kernel's per-inode writeback issues ~8 concurrent Write streams, and readahead issues concurrent prefetch Read streams — each picks the next connection. Per-handle affinity would pin one big file to one flow and is explicitly NOT used.
- Each connection is dialed with the same options as today (TLS, auth interceptors, optional snappy, the #138 `InitialConnWindowBytes`/`InitialStreamWindowBytes`, keepalive, max message size). N connections × per-conn windows multiplies in-flight capacity.

## Components

### 1. Config — `pkg/client/config/rpc.go`

- Add `DefaultRpcConnections = 4`.
- Add field `Connections int` to `RPCConfig` with `mapstructure:"connections" validate:"min=1,max=16"`.
- Wire the default through `NewRPCConfig` exactly like the existing int knobs (constructor literal default + `v.SetDefault("connections", DefaultRpcConnections)`), and add `"connections"` to the rpc env-mirror key list if one exists (mirror the existing knobs' treatment — e.g. `ReadaheadWindow`).
- `1` reproduces today's single-connection behavior.

### 2. Connection pool — `pkg/client/grpc/`

`ClientImpl` currently holds `conn *grpc.ClientConn`. Change to hold a pool:

- `conns []*grpc.ClientConn` (len N; `conns[0]` is the primary).
- An atomic counter for round-robin selection.
- `primaryConn()` returns `conns[0]` (used by handshake/keepalive/subscribe and the existing `File()/Fs()/Volume()/Version()` accessors — they keep returning stubs on the primary).
- `DataFileClient() proto.RpcFileClient` returns an `RpcFileClient` bound to the next round-robin connection (`conns[atomic.Add % N]`). This is the ONLY new spread accessor; it is used solely for Read/Write streams.
- The session-id / keepalive interceptors apply to ALL connections (every connection must stamp `session_id` so the shared session resolves). The dial options (including interceptors) are identical per connection.
- `Close()` closes all N connections.

Keep `DataFileClient` minimal: round-robin index, build (or cache per-conn) the `proto.NewRpcFileClient(conns[i])` stub. Per-conn stubs may be precomputed at construction (`fileClients []proto.RpcFileClient`, one per conn) so `DataFileClient` is just an indexed read — cheaper and lock-free.

### 3. Factory — `pkg/client/grpc/factory.go`

`NewClientFromConfig` builds N connections instead of one, all with the same dial options. The session handshake runs once on the primary after all connections are dialed. A failure dialing any connection fails construction (same as today's single-dial failure).

### 4. Data-plane spread point — `pkg/client/io/backend_grpc.go`

The handle's Read (`h.fileClient.Read(...)`, two sites) and `streamingWrite` (`h.fileClient.Write(...)`) currently use the per-handle `fileClient` (primary). Change these THREE stream-creation sites to use `h.client.DataFileClient()` per call (inside the existing `retryOp` closure, so a retry re-picks a connection). Open/Create/Release and all other fd-ops keep using `File()` (primary). The handle no longer needs a pinned `fileClient` for Read/Write (it still uses `h.client` for retryOp); if `fileClient` becomes unused after this change, remove it.

## Error handling

- Each `grpc.ClientConn` independently auto-reconnects (gRPC transport). A Read/Write issued on a momentarily-unavailable connection surfaces the usual transient error; the existing `retryOp` loop re-issues within `rpc.retry_window`, re-picking a connection via `DataFileClient()`. `WaitForReady(true)` (already set on Read/Write) parks stream-open on a CONNECTING channel rather than burning attempts.
- Session recovery is unchanged: the keepalive/handshake on the primary owns session liveness. A non-primary data connection dropping does not affect the session; only the primary's keepalive drives recovery.
- The pool size is fixed for the client's lifetime (no dynamic resize in this feature).

## Testing

- **Unit (`pkg/client/grpc`):** `DataFileClient` round-robins across N connections (distribution over N calls hits each conn); `File()/Fs()/Volume()/Version()` always return the primary; `Connections=1` yields a single conn and `DataFileClient()==File()`-equivalent (same conn); `Close()` closes all. Factory builds N conns.
- **Unit (`pkg/client/config`):** `Connections` default = 4; validation rejects 0 and >16; explicit value round-trips; env mirror works.
- **Integration (gmountie-perf VM, has /dev/fuse, netem):**
  - **1Gbit:** single-file write and a sequential read with `connections=4` exceed the `connections=1` throughput (proving the single-flow ceiling is lifted). Record both numbers.
  - **100Mbit:** `connections=4` vs `connections=1` shows no throughput regression (link-bound) — the no-regression gate for slow links.
  - `-race` clean; existing e2e (`test/e2e/fs`, `test/e2e/api`) green with the default (4) pool.
- Note: throughput assertions are VM-only (FUSE + netem); the local gate is build/vet/lint + unit tests.

## Out of scope

- Dynamic auto-scaling of the pool size (possible later; the pool exposes a fixed N for now).
- Spreading metadata RPCs across the pool (no throughput benefit; latency-bound).
- Server-side changes (none needed — session is connection-agnostic).
