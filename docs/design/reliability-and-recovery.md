# Reliability and Recovery

**Status:** Shipped (PR #119, 2026-06-14)
**Last updated:** 2026-06-14

The durable record of gMountie's client-side recovery model: how open file
handles survive a `gmountie serve` process restart without an external
database, and without the application ever seeing an error.

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

## 3. Detecting a restart — and distinguishing it from a reap

The client holds a session via `SessionHandshake`
(`pkg/client/grpc/session.go`). On any disconnect it first tries
`Resume(session_id)`. When the server is still up *and* the session is still
alive, `Resume` succeeds and all fds remain valid (a brief network blip,
handled by the existing resilient-retry layer). Otherwise `Resume` returns
`Resumed=false` and the client falls back to `Create`, minting a **new
`session_id`**.

A `session_id` change tells the client its server-side fds are dead — but it
does **not** by itself say *why*, and the two causes need opposite handling:

- **Server restart.** The process died and came back with an empty session
  table. While it was down, no client could write, so the backing file is
  exactly as the last writer left it — reopening by path is as safe as the
  original `Open`. **Reclaim.**
- **Same-process reap.** The server stayed up and reaped this client's idle
  session after it was disconnected past `server.session.grace_period`. Other
  clients may have mutated the file in the meantime, so silently reopening
  would substitute a possibly-changed file into a live fd. **Do not reclaim**
  — let the dead fd fail cleanly (the historical "fail cleanly past grace"
  contract).

The client cannot tell these apart from the `session_id` change alone (both
produce a `Create`). The server therefore mints a **boot epoch** — a random
nonce generated once per process (`AppContext.BootEpoch`, `pkg/server/app.go`)
and returned on every `SessionCreateReply` (`boot_epoch`). The client records
it (`SessionHandshake.BootEpoch()`):

- `Create` reply carries a **different** epoch ⇒ the process restarted ⇒ reclaim.
- `Create` reply carries the **same** epoch ⇒ the same process reaped us ⇒ fail clean.

This is exactly NFSv4's *boot verifier*. Each open file handle
(`grpcFileHandle`) stores a snapshot of both the `session_id` and the
`boot_epoch` at open time, inside an `atomic.Pointer[fdState]` (see §4.2). The
gate is then: a changed `session_id` triggers reclaim **only** when the live
`BootEpoch()` also differs from the handle's snapshot epoch.

> **Read-order invariant.** The handshake publishes the new epoch *before* the
> new `session_id` (under one mutex, in both `Establish` and the Create
> fallback), and `reclaimIfStale` reads `SessionID()` *before* `BootEpoch()`.
> So any reader that observes a changed session id is guaranteed to observe the
> matching new epoch — a restart can never be misread as a reap.

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

The `(fd, sessionID, epoch)` triple is held in a single
`atomic.Pointer[fdState]`:

```go
type fdState struct {
    fd        uint64
    sessionID string
    epoch     string // server boot epoch this fd was opened/reopened against
}
```

This keeps the values consistent without a read lock: any reader that loads
the pointer sees either the pre-reclaim or post-reclaim state atomically,
never a mix. The write side (swap-in) is protected by `reopenMu`; reads
are lock-free. The `epoch` rides along so a *subsequent* reap is judged
against the incarnation the fd was last reopened on, not the original one.

### 4.3 `reclaimIfStale`

```go
func (h *grpcFileHandle) reclaimIfStale(ctx context.Context) fuse.Status
```

1. **Lock-free fast path.** Load the state snapshot and compare
   `cur.sessionID` to `client.SessionID()`. Equal → return `fuse.OK`
   immediately (the common case for a handle that has never gone stale).

2. **Restart-vs-reap gate.** The session changed. Compare `client.BootEpoch()`
   to `cur.epoch`. **Equal → a same-process reap:** return `fuse.OK` *without
   reopening*. The fd-op then proceeds with the dead fd and the server returns
   a clean `NotFound`, so the application gets an honest error instead of a
   silently-substituted file. Only when the epoch **differs** (a true restart)
   does reclaim continue.

3. **Acquire `reopenMu`.** Re-check both the session id and the epoch under
   the lock (a racing caller may have already reclaimed, or the change may have
   resolved into a reap); either re-check returns `fuse.OK` and skips the
   redundant `Open`.

4. **Reopen.** Call `client.File().Open(path, reopenFlags)` under the original
   `reopenCaller`, against the live session, with `grpc.WaitForReady(true)` so
   the call transparently waits for the new server to finish starting up.

5. **Atomic swap.** On success, store a new
   `&fdState{fd: newFd, sessionID: live, epoch: liveEpoch}` via `h.state.Store`.
   All subsequent lock-free reads on this handle see the fresh triple.

6. **Surface failure.** If the path no longer resolves (unlinked-but-open;
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

This relaxation does not cause a reaped fd to loop: on a reap, `reclaimIfStale`
does not reopen, the op sends the dead fd, and the server returns `NotFound` —
a *permanent* (non-retryable) status — so `retryOp` returns it immediately.

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
`rpc.retry_window`. Both default to 60 s. The grace period keeps a session
alive across a brief disconnect so `Resume` reattaches the *same* session and
the fds stay valid. If the grace period is shorter than the retry window, a
blip only slightly longer than the grace gets the session reaped while the
server is otherwise healthy; because that is a same-process reap (epoch
unchanged), the client correctly fails the fd cleanly rather than reopening —
but it is an avoidable loss of availability. Keep grace `≥` window. This is
config, not code.

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
app write() ─▶ grpcFileHandle{fdState: {fd=7, sessionID=A, epoch=E1}}
               ─▶ Write(fd=7, sid=A) ─▶ server(session A, epoch E1)
                                          │ SIGTERM → GracefulStop (drains, exits)
                        UNAVAILABLE ◀─────┘  ... process restarts → epoch E2

SessionHandshake: Resume(A) → Resumed=false → Create → sessionID=B, epoch=E2

client retryOp: WaitForReady reconnects; next attempt:
  reclaimIfStale: state.sessionID(A) ≠ client.SessionID(B)   [stale]
                  client.BootEpoch(E2) ≠ state.epoch(E1)     [restart, not reap]
    → Open(path, reopenFlags, reopenCaller) → new fd=3
    → state.Store(&fdState{fd=3, sessionID=B, epoch=E2})
  retry Write(fd=3, sid=B) ─────────────────────────▶ OK

app write() returns — never saw an error
```

Contrast — a same-process **reap** (server stays up, epoch unchanged):

```
SessionHandshake: Resume(A) → Resumed=false → Create → sessionID=B, epoch=E1
  reclaimIfStale: state.sessionID(A) ≠ client.SessionID(B)   [stale]
                  client.BootEpoch(E1) == state.epoch(E1)    [reap → do NOT reopen]
    → return OK without reopening
  Read(fd=7, sid=A) ─▶ server: session A unknown ─▶ NotFound ─▶ ENOENT
app read() gets a clean error — no silent file substitution
```
