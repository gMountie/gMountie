# Reliability and Recovery

**Status:** Shipped (PR #119, 2026-06-14)
**Last updated:** 2026-06-14

The durable record of gMountie's client-side recovery model: how open file
handles survive a `gmountie serve` process restart without an external
database, without a wire-protocol change, and without the application ever
seeing an error.

Sessions, identity binding, and transport security are covered in
[security-and-transport.md](security-and-transport.md). The cache layer,
which survives restarts independently and continues serving from its on-disk
tier after a reclaim, is covered in
[caching-and-consistency.md](caching-and-consistency.md).

---

## 1. The problem

When a `gmountie serve` process restarts — whether from a rolling update,
a container restart, an OOM-kill, or a crash — the new process starts with
an empty session table and has never heard of any client's server-side file
descriptors. The client's in-flight reads and writes then hit `UNAVAILABLE`
errors; without recovery, the application sees `EIO`/`EBADF` on file
descriptors it still considers valid.

---

## 2. Why there is no external session database

The obvious instinct is to persist sessions and open files in a shared store
and restore them in the new process. This does not work for the hard part:

- **The wire protocol is offset-based.** `Read`/`Write` carry `offset`
  per-RPC; there is no server-side seek position. A server fd is essentially
  `(path, flags)` plus a kernel `*os.File` — bound to the process.
- **A file descriptor cannot outlive its process.** When the server exits,
  the kernel reclaims every fd it holds. "Restore open files from a
  database" therefore means *reopen by path in the new process*. The
  `(path, flags)` to do that reopen is information the **client** already
  holds, because the client issued the original `Open`.

A database would store what the client already has, while adding a hot-path
dependency and a new failure domain. The proven design for this class of
problem is **NFSv4 server-reboot recovery**: the client is the authority on
"what I have open," and on detecting a server restart it *reclaims* its
state by replaying opens against the new server.

---

## 3. The restart signal

The client already detects a server restart — no new wire-level epoch is
needed.

The client holds a session via `SessionHandshake`
(`pkg/client/grpc/session.go`). On any disconnect it first tries
`Resume(session_id)`. When the server is still up, `Resume` succeeds and
all fds remain valid (a brief network blip, handled by the existing
resilient-retry layer). A **fresh server process has an empty session
table**, so `Resume` returns `Resumed=false` and the client falls back to
`Create`, minting a **new `session_id`**.

That `session_id` change *is* the restart signal. It changes exactly when
server-side fds are dead (restart, or a grace-period reap) and stays stable
when `Resume` succeeds. A new server cannot know the old id, so a true
restart always produces a different id.

Each open file handle (`grpcFileHandle`) stores a snapshot of the
`session_id` taken at open time, as part of an `atomic.Pointer[fdState]`
(see §4.2). The predicate `h.state.Load().sessionID != client.SessionID()`
is therefore a precise, per-handle "my server fd is stale" test.

---

## 4. Client reclaim mechanics

Reclaim is **lazy and per-handle**: a handle reopens itself the first time
it is used after going stale. There is no central registry and no eager
enumeration of all open handles.

### 4.1 Sanitized reopen flags

`grpcFileHandle` stores a `reopenFlags uint32` produced by
`sanitizeReopenFlags` at construction time (`pkg/client/io/reclaim.go`).
The sanitizer strips `O_CREAT | O_EXCL | O_TRUNC` and preserves the access
mode and `O_APPEND`:

- `O_TRUNC` on reopen would **discard the bytes the application is
  mid-write on** — data loss.
- `O_EXCL` would fail immediately because the file already exists.
- `O_CREAT` is unnecessary; the file was already created.
- `O_APPEND` is preserved so write semantics remain identical.

A handle originally opened via `Create` therefore reclaims via **`Open`**
with the sanitized flags, never via `Create`.

The handle also stores the **original opener's identity** as `reopenCaller`,
so the reopened fd is created under the same principal as the original,
preserving server-side identity enforcement.

### 4.2 The `fdState` atomic snapshot

The `(fd, sessionID)` pair is held in a single `atomic.Pointer[fdState]`:

```go
type fdState struct {
    fd        uint64
    sessionID string
}
```

This keeps the pair consistent without a read lock: any reader that loads
the pointer sees either the pre-reclaim or post-reclaim state atomically,
never a mix. The write side (swap-in) is protected by `reopenMu`; reads
are lock-free.

### 4.3 `reclaimIfStale`

```go
func (h *grpcFileHandle) reclaimIfStale(ctx context.Context) fuse.Status
```

1. **Lock-free fast path.** Load the state snapshot and compare
   `cur.sessionID` to `client.SessionID()`. Equal → return `fuse.OK`
   immediately (the common case for a handle that has never gone stale).

2. **Acquire `reopenMu`.** Re-check the predicate under the lock. A racing
   caller may have already reclaimed; the re-check avoids a redundant
   `Open` RPC.

3. **Reopen.** Call `client.File().Open(path, reopenFlags)` against the
   live session, with `grpc.WaitForReady(true)` so the call transparently
   waits for the new server to finish starting up.

4. **Atomic swap.** On success, store a new `&fdState{fd: newFd, sessionID: live}`
   via `h.state.Store`. All subsequent lock-free reads on this handle see
   the fresh pair.

5. **Surface failure.** If the path no longer resolves (unlinked-but-open;
   see §6), reclaim fails and the error surfaces to the application.

### 4.4 Wiring into every fd-op

`reclaimIfStale` is called at the top of every fd-carrying operation's
retry closure: `Read`, `Write` (streaming), `Flush`, `Fsync`, `Release`,
`Allocate`, `GetLk`/`SetLk`/`SetLkw`, `CopyFileRange`, `Lseek`, and the
coalescer drain path (`drainCoalescer`). Because it runs **per retry
attempt**, it benefits from `WaitForReady` reconnect and the retry window:
coalesced writes buffered on a stale handle replay to the new fd rather than
being dropped.

### 4.5 Relaxed `classFdOp` retry guard

Before session reclaim, `retry.go` stopped retrying a `classFdOp` on a
session-id change ("fd dead / replay-unsafe"). With reclaim in place, the fd
self-heals, so `classFdOp` now keeps retrying across a session change just
like `classIdempotentRead`. `classPathMutation` still bails on a session
change (no fd to reopen; replay-unsafe against the new session's empty
idempotency table).

---

## 5. Server side

No new server code is required.

On a planned restart, `SIGTERM`/`SIGINT` routes through `signal.NotifyContext`
→ `ctx.Done()` → `GracefulStop()` (`pkg/server/grpc/server.go`).
`GracefulStop` stops accepting new RPCs and **blocks until all in-flight
RPCs complete**, including active streaming `Read`/`Write` frames. Planned
restarts therefore drain cleanly; the client's in-flight op completes before
the old server exits, so the client never sees a torn write.

A crash skips the drain. In-flight bytes from the crashed op are lost
(the application's `write()` was not `fsync`'d); reclaim recovers the open
file and subsequent ops succeed.

**Config invariant:** `server.session.grace_period` must be `≥` the client's
`rpc.retry_window`. Both default to 60 s. If the grace period is shorter
than the retry window, a session can be reaped while the client is still
trying to reclaim within the window, causing spurious errors. This is config,
not code.

---

## 6. Deployment requirements

Reclaim works by reopening files at their original paths in the new server
process. This requires:

1. **Same underlying storage on restart.** The new process must serve the
   same volume data directory the original process served, so the paths
   the client tries to reopen exist and have their previous content.

2. **Exclusive serialized handoff.** The supervisor must ensure the old
   process releases the storage before the new one opens it for writing.
   Concurrent read-write access from two server processes is not safe.

3. **Restart gap fits inside the retry window.** The time from the old
   process exiting to the new process accepting RPCs must fit within
   `rpc.retry_window` (default 60 s). The client retries with
   `WaitForReady(true)` throughout.

These are generic operational constraints. Verifying that a specific
deployment satisfies them is an operational task for that deployment.

---

## 7. Limitations (Design B — deferred)

Two cases cannot be recovered by reopen-by-path:

- **Byte-range locks are not re-asserted.** A reopened fd holds no
  `fcntl`/`flock` locks. While a single client holding a volume has no
  second contender during the restart gap (making this acceptable in
  practice today), correct multi-client lock semantics across a restart
  requires a server-side grace window that accepts reclaim opens and lock
  re-assertions before allowing new conflicting opens from other clients.
  This is Design B, deferred until concurrent multi-client lock contention
  is a real workload.

- **Unlinked-but-open files cannot be recovered.** If a file was `unlink`'d
  while still held open by the client, the path is gone on disk and
  reopen-by-path fails. The reclaim error surfaces to the application.
  NFS handles this with a silly-rename (`.nfsXXXX`); that is out of scope
  here.

---

## 8. Data flow — planned restart

```
app write() ─▶ grpcFileHandle{fdState: {fd=7, sessionID=A}}
               ─▶ Write(fd=7, sid=A) ─▶ server(session A)
                                          │ SIGTERM → GracefulStop (drains, exits)
                        UNAVAILABLE ◀─────┘

SessionHandshake: Resume(A) → Resumed=false → Create → live sessionID=B

client retryOp: WaitForReady reconnects; next attempt:
  reclaimIfStale: state.sessionID(A) ≠ client.SessionID(B)
    → Open(path, reopenFlags) → new fd=3
    → state.Store(&fdState{fd=3, sessionID=B})
  retry Write(fd=3, sid=B) ─────────────────────────▶ OK

app write() returns — never saw an error
```
