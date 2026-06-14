# Session reclaim across server restarts — Design A

**Date:** 2026-06-14
**Status:** Approved design (refined during planning — see "Refinement")
**Repo:** `gMountie` (OSS core)
**Scope:** Make open files survive a `gmountie serve` process restart, driven by client-side reclaim. No external database, no wire-protocol change.

---

## Problem

When a `gmountie serve` process restarts (container/service restart, OOM-kill, crash, or a `systemctl restart`), every open session and every open file is lost. The new process starts with an empty session table and has never heard of the client's server-side fd handles. The client's in-flight reads/writes then fail, and the user's application sees `EIO`/`EBADF` on file descriptors it still considers valid. Any orchestrated deployment that restarts the server (rolling updates included) hits this.

## The crux (why an external session database is the wrong tool)

One instinct is to persist sessions and open files in an external database and restore them on the new process. This does not work for the hard part:

- **The wire protocol is offset-based.** `Read`/`Write`/`WriteAndFlush` carry `offset` per-RPC (`api/proto/file.proto`); there is no server-side seek position. A server fd is essentially `(path, flags)` plus a process-bound `*os.File` handle (`FileEntry`, `pkg/server/service/session.go`).
- **A file descriptor cannot outlive its process.** You cannot serialize `*os.File` into a database and "restore" it in a new process. When the process dies, the kernel reclaims the fd. "Restore open files from the DB" can therefore only ever mean **reopen by path on the new process** — and `(path, flags)` is information the *client* already holds, because the client issued the `Open`.

So a database would store metadata the client already has, while adding a stateful dependency on the filesystem hot path (every `Open`/`Release`/lock round-tripping to it) and a new failure domain (DB down → every mount down). It pays a high price and still does not solve fd restoration.

The proven design for this problem is **NFSv4 server-reboot recovery**: the client is the source of truth for "what I have open," and on detecting a server restart it *reclaims* its state by replaying opens against the new server. State authority lives at the client, not in a server-side store.

## Refinement — the restart signal already exists

Planning-time code review (`pkg/client/grpc/session.go`, `pkg/client/io/retry.go`, `pkg/client/io/backend_grpc.go`) showed the client **already detects a server restart** — we do not need a new wire-level "boot epoch":

- The client holds a session via `SessionHandshake`. On any disconnect it tries `Resume(session_id)`; on success the server still has the session and fds stay valid (a brief network blip, already handled). A **fresh server process has an empty session table**, so `Resume` returns `Resumed=false` and the client falls back to `Create`, minting a **new `session_id`** (`session.go`, the "open fds are now invalid" path).
- **The `session_id` change *is* the restart signal.** It changes exactly when the server-side fds are dead (restart, or a grace-period reap) and stays stable when `Resume` succeeds. A new server cannot know the old id, so a true restart always changes it.
- fd-carrying RPCs already put a **snapshot** of the session id (`h.sessionID`, captured at `Open` time) on the wire, while `Open`/`Create` use the **live** `client.SessionID()`. So `h.sessionID != client.SessionID()` is a precise, per-handle "my fd is stale" predicate — it fires both for an op already in flight during recovery *and* for a handle's first op issued after recovery already completed.

This collapses the server side to nearly nothing and makes the feature **client-only, with no proto change**.

## Deployment assumption

Reopen-by-path only works if the restarted process sees the **same underlying storage** (the same volume/data directory the original process served), and the supervisor must give an **exclusive serialized handoff** (old process releases the storage before the new one writes), so the two never have it open read-write at once. The restart gap (old exits → new ready) must fit inside the client retry window. These are generic deployment requirements; verifying a specific orchestrated deployment satisfies them is an operational task for that deployment, not part of this OSS design.

## Design A

### Server — already done

The graceful path already exists and needs no new code:

- `SIGTERM`/`SIGINT` route through `signal.NotifyContext` → `ctx.Done()` → `s.Stop(true)` → gRPC `GracefulStop()` (`cmd/commands/serve.go`, `pkg/server/app.go`, `pkg/server/grpc/server.go`). `GracefulStop` stops accepting new RPCs and **blocks until in-flight RPCs finish — including active streaming `Read`/`Write`** — so planned restarts drain cleanly (no torn in-flight ops). A crash skips this; reclaim still recovers the files, losing only the unflushed in-flight bytes from the crash (same as any process crash on a local fs).

The one operational invariant (already stated in code comments): `server.session.grace_period` should be `≥` the client's `rpc.retry_window` so a transparent resume holds for the whole window. Both default to 60s. This is config, not code.

### Client — lazy per-handle reclaim

Reclaim is **lazy and per-handle**: a handle reopens itself the next time it is used after going stale. No central registry, no eager enumeration.

1. **Retain reopen flags.** Add a sanitized `reopenFlags` to `grpcFileHandle` (it currently keeps `path` and `fd` but not `flags`). Sanitize at construction: strip `O_CREAT | O_EXCL | O_TRUNC` (re-applying `O_TRUNC` would truncate the file the app is mid-write on — data loss; `O_EXCL` would fail since the file now exists) and preserve the access mode and `O_APPEND`. A `Create`'d handle therefore reclaims via **`Open`**, never `Create`.

2. **`reclaimIfStale(ctx)` on the handle.** Under a per-handle mutex (so concurrent ops on the same handle reopen once, re-checking the predicate under the lock): if `h.sessionID != h.client.SessionID()`, call `client.File().Open(path, reopenFlags)` against the live session (`grpc.WaitForReady(true)`), and on success swap in the new `fd` and `sessionID`. If the path no longer resolves (unlinked-but-open), reclaim fails and the error surfaces.

3. **Invoke it inside each fd-op attempt.** Call `reclaimIfStale` at the top of every fd-op's `retryOp` closure — `Read`, `Write` (streaming), `Flush`, `Fsync`, `Release`, `Allocate`, `GetLk`/`SetLk`/`SetLkw`, `CopyFileRange`, `Lseek`, and the coalescer drain path (`drainCoalescer`). Because it runs per attempt, it benefits from `WaitForReady` reconnect and the retry window, and **coalesced writes buffered on a now-stale handle replay to the new fd** instead of being dropped.

4. **Relax the retryOp guard for `classFdOp`.** Today `retry.go` stops retrying a `classFdOp` on a session-id change ("fd dead / replay-unsafe"). With reclaim that reason no longer holds — the fd self-heals — so `classFdOp` should keep retrying within the window like `classIdempotentRead`. `classPathMutation` keeps stopping (no fd to reopen; replay-unsafe against the new session's empty idempotency cache).

All of this happens inside `rpc.retry_window` (default 60s). If reclaim cannot complete within the window (server never returns, or the path is gone), the client surfaces the error as it does today.

### Data flow (planned restart)

```
app write() ─▶ grpcFileHandle{fd=7, sessionID=A} ─▶ Write(fd=7, sid=A) ─▶ server(session A)
                                                                              │ SIGTERM → GracefulStop
                                                          UNAVAILABLE ◀───────┘ (drains in-flight, exits)
SessionHandshake: Resume(A) → Resumed=false → Create → sessionID=B (new process, empty table)
client retryOp: WaitForReady reconnects; next attempt:
  reclaimIfStale: h.sessionID(A) != client.SessionID(B) ─▶ Open(path, reopenFlags) ─▶ new fd=3
  swap handle → {fd=3, sessionID=B}
  retry Write(fd=3, sid=B) ─────────────────────────────────────────────────▶ OK
app write() returns ───────────────────────────────────────────────────────── (never saw an error)
```

## Error handling & edge cases

- **Flag sanitization (data-loss guard):** reopen must strip `O_CREAT|O_EXCL|O_TRUNC` and keep the access mode + `O_APPEND`. Reopening with the raw open flags would truncate or fail. Covered by Design.A.1.
- **Concurrent ops on one handle:** the per-handle reopen mutex re-checks the stale predicate under the lock, so N in-flight ops reopen once, not N times. Covered by Design.A.2.
- **Coalesced/buffered writes:** the `WriteCoalescer` may hold unsent bytes when the handle goes stale; routing `drainCoalescer` through `reclaimIfStale` replays them to the new fd. Covered by Design.A.3.
- **Crash mid-write (no drain):** reclaim recovers the open file; unflushed in-flight bytes from the crashed op are lost (the app's `write()` had not been `fsync`'d). Acceptable.
- **Reclaim exceeds retry window:** surface the error as today. No silent hang.
- **Unlinked-but-open files:** the one genuinely hard case — the path no longer resolves, so reopen-by-path fails for a file `unlink`'d while held open. Known, bounded limitation; NFS solves it with silly-rename (`.nfsXXXX`), out of scope here.
- **Byte-range locks (`SetLk`/`SetLkw`):** the *lock state* is not re-asserted in Design A — a reopened fd holds no locks. When a single client holds a volume there is no second contender during the restart gap, so this is acceptable for now. Correct cross-client lock recovery is Design B.
- **Concurrent-writer safety:** out of scope — the supervisor enforces exclusive serialized handoff (see Deployment assumption).

## Non-goals

- No external/shared session database.
- No wire-protocol / proto change, no server "boot epoch".
- No persistence of `session_id` or any credential in shared state.
- No cross-client lock recovery (Design B).
- No recovery of unlinked-but-open files.
- No arbitration of concurrent writers — that is the supervisor's responsibility.

## Testing

- **Unit (flag sanitizer):** `O_RDWR|O_CREAT|O_TRUNC` → `O_RDWR`; `O_WRONLY|O_APPEND` → `O_WRONLY|O_APPEND`; `O_RDONLY` → `O_RDONLY`. Testify suite.
- **Unit (`reclaimIfStale`):** with a fake client whose `SessionID()` differs from the handle snapshot, one op reopens and updates `fd`+`sessionID`; a not-stale handle is a no-op; N concurrent callers trigger exactly one `Open` (assert call count). Test the property the fix protects (fd remapped, single reopen), not a guessed error shape.
- **Unit (retryOp guard):** a `classFdOp` whose closure first errors `Unavailable` then succeeds after a simulated session change returns success (no early bail); a `classPathMutation` still bails on session change.
- **E2E (`test/e2e/`, real FUSE mount):** mount a volume, hold a file open and mid-write, restart the server process in-place (same data dir, new session table), assert the application's `read()`/`write()` resume without error and data is intact. CI has `/dev/fuse` so the in-process variant runs in CI; gate any VM-only variant behind the existing env flags.

## Design B — future work (documented, not built)

Server **grace window + lock reclaim**, the NFSv4-grade end state. On boot the server opens a *grace window* during which it accepts only reclaim opens + **lock re-assertions** and blocks new conflicting opens/locks from *other* clients until grace expires; the client replays its byte-range locks during reclaim. This buys correct multi-client lock semantics across a restart. Deferred until there is concurrent multi-client access to a volume to make lock contention real. Design A's per-handle reclaim is the foundation B builds on.

The only other legitimate use for a shared store here is **session observability** (listing active sessions across server instances) — explicitly deferred, and it would never sit on the read/write hot path.

## Roadmap note

When implemented, the durable record lands in `docs/design/` (a reliability/recovery section, alongside `security-and-transport.md` and `caching-and-consistency.md`), and a one-line row goes in `docs/roadmap.md`. This transient spec is pruned after consolidation.
