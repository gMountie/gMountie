# gMountie Architecture & Protocol Overview

**Status:** Living document
**Last updated:** 2026-06-09

This document describes how gMountie is shaped today: the pieces, the wire
protocol's surface, and the contracts each side relies on. No code-level
detail — for that, the package READMEs and source comments are the source
of truth.

For UID/GID semantics and the identity model see
[identity-and-permissions.md](identity-and-permissions.md) — the durable
record of the shipped identity feature.

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
| client process     |   gRPC / TLS     | server process  |
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
- **Wire** is gRPC over HTTP/2, TLS-terminated. A custom Snappy codec is
  available as an opt-in compressor (`rpc.compression: snappy`, default
  `none` — see §8). Reflection is opt-in (default off); gRPC keepalive at
  the connection level is configured by gRPC itself.

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
metrics — are gRPC interceptors. Identity resolution and per-op
credential binding (UID/GID handling) happen in the service/io layers
and are the subject of their own design document
([identity-and-permissions.md](identity-and-permissions.md)).

### 3.2 The client

Smaller and simpler. One mount model:

- **Single-volume mount** (`mount.SingleVolumeMounter`). One remote
  volume mounted at one local path. This is what `gmountie mount`
  produces, and the only `mount.type` the config accepts (`single`).

(The earlier multi-volume/`MemFS` mounter that backed the desktop UI was
extracted from this repo for a future separate desktop repository.)

The mount itself is FUSE-bound behind a platform-independent backend seam: Linux
mounts via `hanwen/go-fuse`, macOS via a cgofuse adapter over the same seam. See
[macos-mount.md](macos-mount.md).

The client also runs a small gRPC client wrapper that:
- Holds the single connection to the server.
- Establishes a session at connect time.
- Maintains a background keepalive stream and self-heals it on
  disconnect.

### 3.3 The wire protocol

All cross-process state lives in protobuf messages. Five services
partition the surface:

| Service | Purpose | Examples |
|---|---|---|
| `SessionService` | Connection-level session lifecycle. | `Create`, `Resume`, `Keepalive`, `WhoAmI` |
| `VolumeService` | Discovery — what volumes does this server expose? | `List`, `Resolve` |
| `VersionService` | Protocol version negotiation. | `Get` (negotiates parameters like `frame_size_bytes`) |
| `RpcFs` | Path-keyed filesystem operations, plus the server-push invalidation stream. | `GetAttr`, `SetAttr`, `ReadDir`, `Mkdir`, `Rename`, `Unlink`, `StatFs`, `GetXAttr`, `Access`, `Subscribe` |
| `RpcFile` | Fd-keyed file operations. | `Open`, `Create`, `Read`, `Write`, `Release`, `Flush`, `Fsync`, `GetLk`/`SetLk`/`SetLkw`, `Allocate` |

The split between `RpcFs` and `RpcFile` is meaningful: `RpcFs` operates
on paths and is stateless on the server beyond the volume's filesystem;
`RpcFile` operates on file descriptors that the server allocates and
tracks per session.

Several RPCs stream on the wire. `RpcFile.Read` is server-streaming and
`RpcFile.Write` is client-streaming, so a single RPC frames many
in-flight chunks without per-chunk round-trips. `RpcFs.ReadDir` is
server-streaming so a directory listing of any size never hits the unary
message cap. `RpcFs.Subscribe` is a server-stream the client holds open
to receive push invalidations. `SessionService.Keepalive` is a
server-stream the client holds open as a liveness signal and carries no
payload.

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
   "disconnected" and starts a grace-period timer (`server.session.grace_period`, default 60 s).
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
- **An idempotency cache.** A per-session LRU (default 4096 entries, `server.session.idempotency_cache_size`) keyed by `request_id` (UUID
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

The server's interceptor chain (logging, auth, prometheus metrics) runs
uniformly across all volumes — at that level the volume name is just a
routing parameter. The security boundary is enforced when the volume is
resolved: the authenticated principal must pass the per-volume ACL
(`auth.users[].volumes` / `auth.default_allow`), and the filesystem
handed back is bound to the principal's resolved identity (see §7 and
the identity design doc).

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
- **Metadata** ops (GetAttr, ReadDir, Mkdir, Rename, etc.): 5 s default.
- **I/O** ops (Read, Write, Allocate): 30 s default.

Both are config-driven. A stalled server fails the RPC instead of
hanging the FUSE thread.

### 6.2 Retry

The client retries idempotent operations and idempotency-token-stamped
mutating operations on transient gRPC errors (`Unavailable`,
`DeadlineExceeded`). The wall-clock budget for the whole operation is
`rpc.retry_window` (default 60 s; `0` = fail-fast / single attempt).
Backoff is exponential: 100 ms initial delay, capped at 1 s per sleep;
each attempt gets its own per-attempt deadline (`timeout_meta` /
`timeout_io`). The `request_id` on mutating ops is generated **once per
logical operation** and reused across retries — this is what makes retry
safe within the same session. On a session-id change (fresh `Create`
after `Resume` fails), idempotent reads keep retrying; fd ops and path
mutations stop immediately because their state is gone.

### 6.3 Idempotency on the server

Mutating RPCs (`Open`, `Create`, `Write`, `WriteAndFlush`,
`CopyFileRange`, `Mkdir`, `Rmdir`, `Rename`, `Symlink`, `Unlink`,
`SetAttr`, `SetXAttr`, `RemoveXAttr`) carry a `request_id`. The
server's per-session LRU (default 4096 entries) caches the successful reply keyed by
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
- **A canonical `FsError` enum** (`api/proto/common.proto`) carried in the
  reply struct for filesystem errors: `FS_ENOENT`, `FS_EACCES`,
  `FS_ENOTEMPTY`, etc. It is **OS-neutral** — the wire never carries a raw OS
  errno number. The server maps its native errno → `FsError`; each client
  mount adapter maps `FsError` → its own host kernel's errno.

The mapping lives in `pkg/common/fserr`, built on Go's `syscall.Errno` (whose
`E*` constants are already per-GOOS-correct), so one table serves both the
go-fuse (Linux) and cgofuse (macOS) adapters host-correctly; only codes whose
*name* differs across OSes need a build-tagged shim (`FS_ENO_XATTR` → Linux
`ENODATA` / Darwin `ENOATTR`). This is why errno *numbers* never go on the wire:
a Linux `ENOTEMPTY` (39) must reach the macFUSE kernel as Darwin's 66, not as
Darwin errno 39. The model mirrors NFS `NFSERR_*` / gRPC's canonical codes — and
means a non-Linux server would map *its* native errno into the same enum with no
client change.

The client distinguishes the two layers: a gRPC error is the wire failing
(retry candidate); a non-OK `FsError` is the filesystem refusing (propagate to
the kernel as the corresponding host errno).

## 7. Authentication and identity

All requests are authenticated. Two modes ship, selected by `auth.type`,
at the gRPC interceptor layer:

- `basic` — username/password (argon2id at rest) against a configured
  user list. Verification runs once per session; later RPCs authorize by
  `session_id` (session-scoped auth).
- `mtls` — the client's TLS certificate carries its identity, verified
  during the handshake (cert CN as principal).

The authenticated **principal drives permission decisions**: per request
the server resolves the principal through the volume's identity mapping
(`squash` default / `static` / `system` / `passthrough`) and binds the
volume filesystem to the resolved identity, so every operation runs
under the principal's credentials and the **kernel** enforces
permissions natively. The UID/GID the client forwards on the wire is
advisory display data, never an authority. Per-volume ACLs
(`auth.users[].volumes`, `auth.default_allow`) gate which volumes a
principal can touch at all. Full model:
[identity-and-permissions.md](identity-and-permissions.md) and
[security-and-transport.md](security-and-transport.md).

TLS at the transport layer ships as well: the server auto-generates a
certificate, and the client chooses a verification mode (`verify`,
`tofu`, or `insecure`). A renewed server cert+key on disk is picked up
live at the next handshake (leaf live-reload) — no restart.

## 8. Compression

Compression is **opt-in and off by default** (`rpc.compression: none`).
A custom Snappy codec is registered server-side (gzip is registered as
well); a client sets `rpc.compression: snappy` to compress every RPC on
the connection. Snappy is chosen for speed-over-ratio: it pays off on
slow WAN links, while on a fast link the compressor itself becomes the
bottleneck — see [performance.md §2.7](performance.md) for the
measurements behind the default.

## 9. What this protocol intentionally does not do

> **Since implemented.** Several items this section originally listed as
> "not done" have shipped. Streaming `Read`/`Write` (no more 4 MiB
> ceiling) and frame-size negotiation are documented in
> [performance.md](performance.md). The client-side cache and the
> server-push `Subscribe` invalidation channel are documented in
> [caching-and-consistency.md](caching-and-consistency.md).

The protocol still does not do:

- **No POSIX advisory locks across sessions.** `GetLk`/`SetLk`/`SetLkw`
  exist on the wire and pass through to the loopback FS for a single
  client, but no server-side lock manager coordinates between multiple
  clients sharing a volume. Multi-client lock coordination is not
  implemented.
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
  which the session is still resolvable via `Resume`
  (`server.session.grace_period`, default 60 s).
- **Loopback filesystem** — the go-fuse helper the server uses to turn
  protocol ops into ordinary host-kernel syscalls under the volume's
  path.
- **Principal** — the authenticated identity from the auth interceptor
  (basic-auth username or mTLS cert CN). Resolved through the volume's
  identity mapping and enforced kernel-natively on every operation (§7).
  Distinct from the UID/GID the client forwards on each call, which is
  advisory display data.
