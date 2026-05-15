# gMountie Architecture & Protocol Overview

**Status:** Living document
**Last updated:** 2026-05-15

This document describes how gMountie is shaped today: the pieces, the wire
protocol's surface, and the contracts each side relies on. No code-level
detail — for that, the package READMEs and source comments are the source
of truth.

For UID/GID semantics and the identity model see
[identity-and-permissions.md](identity-and-permissions.md). That doc is
forward-looking; this one describes what the codebase actually does.

## 1. What gMountie is

gMountie exposes server-side directories — called **volumes** — over a
gRPC connection. A client mounts a chosen volume locally via FUSE; every
syscall the kernel routes through the FUSE mount is translated to a gRPC
call against the server, which delegates to a local loopback filesystem
under the volume's path.

The goal is "NFS over the internet without a VPN": single TCP connection,
TLS-capable, authenticated, with the usual reliability primitives
(timeouts, retries, idempotency) baked into the protocol rather than
glued on top.

## 2. Logical model

```
+--------------------+                  +-----------------+
| client process     |   gRPC + Snappy  | server process  |
|                    | <==============> |                 |
|  FUSE mount  <----+|                  |+----> loopback  |
|  (/mnt/photos)    ||                  ||      FS under  |
|                   ||                  ||      volume    |
|  io.LocalFileSystem|                  ||      path      |
|  translates FUSE   |                  ||                |
|  ops <-> gRPC      |                  ||  controller    |
+--------------------+                  +-----------------+
```

- **Client** runs `gMountie mount <volume> <mountpoint>`, opens a FUSE
  mount via [go-fuse](https://github.com/hanwen/go-fuse), and translates
  every FUSE op into a gRPC call.
- **Server** runs `gMountie serve`, exposes a configured list of named
  volumes, and answers gRPC calls by routing them to a loopback
  filesystem rooted at the volume's local path.
- **Wire** is gRPC over HTTP/2 with the Snappy codec as a custom
  compressor. Reflection is on; gRPC keepalive at the connection level is
  configured by gRPC itself.

There is no NFS, no VPN, no shared filesystem of any kind between the
two ends. Anything not explicitly modelled in the proto cannot cross.

## 3. Components

### 3.1 The server

The server is a layered application:

- **Controllers** are the gRPC handlers. They translate gRPC requests to
  go-fuse calls and back, and handle protocol-level concerns:
  session resolution, idempotency, fd lookup, error code translation.
- **Services** are stateless coordinators. The two notable ones today
  are the `VolumeService`, which resolves a volume name to a filesystem,
  and the `SessionManager`, which tracks active sessions and their
  per-session state.
- **IO** is the layer that actually touches files. It wraps go-fuse's
  loopback filesystem so that every read/write/stat ultimately lands on
  the host kernel.

Cross-cutting concerns — request logging, auth, optional Prometheus
metrics — are gRPC interceptors. Identity middleware (UID/GID handling)
runs per-request and is the subject of its own design document.

### 3.2 The client

Smaller and simpler. Two mount modes:

- **Single-volume mount.** One remote volume mounted at one local path.
  This is what `gMountie mount` produces.
- **Multi-volume mount.** An in-memory `MemFS` root mounted at one local
  path, with each requested remote volume attached as a subdirectory
  underneath. This mode exists to support the desktop UI (deferred until
  the rest of the project stabilises) which wants several volumes under
  one parent.

The client also runs a small gRPC client wrapper that:
- Holds the single connection to the server.
- Establishes a session at connect time.
- Maintains a background keepalive stream and self-heals it on
  disconnect.

### 3.3 The wire protocol

All cross-process state lives in protobuf messages. Four services
partition the surface:

| Service | Purpose | Examples |
|---|---|---|
| `SessionService` | Connection-level session lifecycle. | `Create`, `Resume`, `Keepalive` |
| `VolumeService` | Discovery — what volumes does this server expose? | `List` |
| `RpcFs` | Path-keyed filesystem operations. | `GetAttr`, `OpenDir`, `Mkdir`, `Rename`, `Unlink`, `Truncate`, `Chmod`, `Chown`, `StatFs`, `GetXAttr`, `Access` |
| `RpcFile` | Fd-keyed file operations. | `Open`, `Create`, `Read`, `Write`, `Release`, `Flush`, `Fsync`, `GetLk`/`SetLk`/`SetLkw`, `Allocate` |

The split between `RpcFs` and `RpcFile` is meaningful: `RpcFs` operates
on paths and is stateless on the server beyond the volume's filesystem;
`RpcFile` operates on file descriptors that the server allocates and
tracks per session.

No service holds streaming RPCs that carry payload — all file I/O is
unary today. The only stream on the wire is `SessionService.Keepalive`,
which is a server-stream the client holds open as a liveness signal and
carries no payload.

## 4. Sessions

A **session** is the unit of per-connection state on the server. It is
established once when the client connects and reaped (after a grace
period) when the keepalive stream breaks. Every fd-carrying RPC, and
every mutating RPC, is scoped to a session id.

### 4.1 Lifecycle

1. Client calls `SessionService.Create`; server allocates a UUID v4 and
   stores it in an in-memory registry along with an empty per-session
   state.
2. Client immediately opens `SessionService.Keepalive(session_id)` and
   leaves the stream open. The server emits a periodic heartbeat
   (currently every 10 s) so that a half-broken TCP connection surfaces
   faster than gRPC's own keepalive would.
3. When the stream ends — for any reason — the server marks the session
   "disconnected" and starts a grace-period timer (default 30 s).
4. While the timer is running, the session is still resolvable: the
   client can `Resume(session_id)` to cancel the timer and reattach. The
   session's open fds remain valid.
5. If the timer fires before `Resume` lands, the session is reaped: its
   fd table is released, idempotency cache discarded, registry entry
   gone.

### 4.2 Client-side recovery

The client treats the keepalive stream as its disconnect detector. If
`stream.Recv()` returns an error, the client:

1. Calls `Resume(currentID)`. If the server says `resumed: true`, the
   client reopens the keepalive stream with the same session id;
   previously-issued fds remain valid.
2. If `Resume` returns `resumed: false`, the server has already reaped
   the session. The client falls back to a fresh `Create`, stores the
   new session id, and reopens the keepalive stream. **Previously open
   fds are now invalid** — the next read/write against one will return
   `NotFound` from the server and the client surfaces `EIO` to the
   kernel. The kernel will see an EIO and the userspace app can reopen
   the file.
3. Both Resume and Create can fail; the client retries with capped
   exponential backoff (200 ms → 5 s).

### 4.3 What the server keeps per session

- **An fd table.** `Open` and `Create` allocate a server-side fd
  (monotonic per session) and register the underlying `nodefs.File`.
  Read/Write/Release/etc. look up the fd via `(session_id, fd_number)`.
  `Release` removes the entry. Session reap releases every remaining
  entry.
- **An idempotency cache.** A 256-entry LRU keyed by `request_id` (UUID
  v4) per session. Used by mutating RPCs — see §6.
- **No file content.** All bytes are read from / written to the
  loopback filesystem; the server holds no file buffers between calls.

### 4.4 Out of scope: durable sessions

A session's state lives in server memory. If the server process
restarts, all sessions are gone — clients fall back to `Create` and
re-open. There is no on-disk session journal today. This is a
deliberate scoping decision; see the roadmap document for where it
might land.

## 5. Volume routing

Every RPC carries a volume name. The server looks the name up in the
volume service per request; there is no per-connection "current volume"
state. A single client connection (and a single session) can use any of
the volumes the server exposes.

The server's middleware chain (logging, auth interceptors, identity
middleware, prometheus metrics) runs uniformly across all volumes — at
this level the volume name is just a routing parameter, not a security
boundary. Per-volume security policy is a planned addition (see the
identity design doc).

## 6. Reliability primitives

The protocol carries three reliability fields that the client populates
and the server honours:

| Field | On which RPCs | Purpose |
|---|---|---|
| `session_id` | every fd-carrying RPC + every mutating RPC | Server-side scoping and authentication of "this is from a known session". |
| `request_id` (UUID) | every mutating RPC | Server-side idempotency dedup key. |
| Per-call deadline | every RPC (gRPC-native, not a proto field) | Bounds how long a stalled server can block the client. |

### 6.1 Per-call timeouts

Every RPC the client makes runs under a context with a deadline:
- **Metadata** ops (GetAttr, OpenDir, Mkdir, Rename, etc.): 5 s default.
- **I/O** ops (Read, Write, Allocate): 30 s default.

Both are config-driven. A stalled server fails the RPC instead of
hanging the FUSE thread.

### 6.2 Retry

The client retries idempotent operations and idempotency-token-stamped
mutating operations on transient gRPC errors (`Unavailable`,
`DeadlineExceeded`). Bounded exponential backoff: 3 attempts,
100 ms → 1 s. The `request_id` on mutating ops is generated **once per
logical operation** and reused across retries — this is what makes
retry safe.

### 6.3 Idempotency on the server

Mutating RPCs (`Open`, `Create`, `Write`, `Mkdir`, `Rmdir`, `Rename`,
`Unlink`, `Truncate`, `Chmod`, `Chown`) carry a `request_id`. The
server's per-session 256-entry LRU caches the successful reply keyed by
that id. If the same id arrives again, the server returns the cached
reply without re-executing the operation. Concurrent duplicates collapse
via singleflight so the underlying filesystem is touched at most once
per logical request. Errors are deliberately not cached — a transient
failure must be re-executable.

The protocol-level guarantee is:

> If the client retries a mutating RPC with the same `(session_id,
> request_id)`, the server will execute the operation at most once. If
> the original execution succeeded, the retry returns the same reply.

This is what lets the client safely retry `Write` mid-copy on a
flaky link without risking duplicated bytes.

### 6.4 Error model

Two layers of error stack on the wire:
- **gRPC status codes** (`InvalidArgument`, `NotFound`, `Unavailable`,
  `DeadlineExceeded`, `Internal`) for protocol-level failures: empty
  session_id, unknown session, stalled call, transport hiccup.
- **FUSE status integers** carried in the reply struct for filesystem
  errors: `ENOENT`, `EACCES`, `EEXIST`, etc. — the server runs the FUSE
  op against the loopback FS and propagates its return code through.

The client distinguishes the two: a gRPC error is the wire failing
(retry candidate); a non-OK FUSE status is the filesystem refusing
(propagate to the kernel as the corresponding errno).

## 7. Authentication

Two modes today, both at the gRPC interceptor layer:

- `none` — no auth interceptor, all requests accepted. Useful for
  local testing.
- `basic` — username/password against a configured user list. The
  authenticated principal is the basic-auth user; today it isn't yet
  used for permission decisions (those still come from the wire UID/GID
  field — see the identity design doc for the planned shape).

TLS at the transport layer is deliberately disabled today and is a
roadmap item, not an architectural decision. The protocol does not
constrain it.

## 8. Compression

A custom Snappy codec is registered server-side and is the preferred
codec for the I/O RPCs (`Read`, `Write`). Metadata RPCs are typically
small enough that compression is dropped per call. Gzip is registered
too but Snappy is the default — it's chosen for speed-over-ratio
rather than ratio.

## 9. What this protocol intentionally does not do

- **No streaming I/O.** A 4 MiB unary ceiling applies to `Read`/`Write`
  today; very large reads/writes are chunked client-side. Streaming
  RPCs are planned (see roadmap).
- **No client-side cache.** Every `GetAttr` and every `Read` traverses
  the wire today. A path-keyed attribute and content cache is a
  headline future feature.
- **No bidirectional RPCs from server to client.** The server cannot
  push cache invalidations or attribute changes; consistency is
  request-time only. Adding a server-push channel is a known need for
  any future cache.
- **No POSIX advisory locks across sessions.** `GetLk`/`SetLk`/`SetLkw`
  exist on the wire and pass through to the loopback FS for a single
  client, but no server-side lock manager coordinates between multiple
  clients sharing a volume. Multi-client coordination is a Phase 3
  topic.
- **No durable session state.** Sessions live in server memory; server
  restart drops them.

## 10. Glossary

- **Volume** — a named, server-side directory exposed via the protocol.
- **Session** — the unit of per-connection state on the server, owning
  an fd table and idempotency cache. Identified by a UUID v4 allocated
  by the server.
- **fd** (in this context) — a server-side handle returned by `Open` or
  `Create`. Monotonic per session, opaque to the client, only valid
  within its session.
- **request_id** — a UUID v4 the client allocates per mutating RPC.
  Server-side dedup key.
- **Keepalive** — the long-running server-stream from
  `SessionService.Keepalive`. Used as a disconnect detector by the
  client.
- **Grace period** — the window after a keepalive stream ends during
  which the session is still resolvable via `Resume`. Default 30 s.
- **Loopback filesystem** — the go-fuse helper the server uses to turn
  protocol ops into ordinary host-kernel syscalls under the volume's
  path.
- **Principal** — the authenticated identity from the auth interceptor
  (basic-auth username today). Distinct from the UID/GID the client
  forwards on each call, which is currently advisory.
