# Session reclaim across server restarts — Design A

**Date:** 2026-06-14
**Status:** Approved design, pre-implementation
**Repo:** `gMountie` (OSS core)
**Scope:** Make open files survive a `gmountie serve` process restart, driven by client-side reclaim. No external database.

---

## Problem

When a `gmountie serve` process restarts (container/service restart, OOM-kill, crash, or a `systemctl restart`), every open session and every open file is lost. The new process starts with an empty session table and has never heard of the client's server-side fd handles. The client's in-flight reads/writes then fail, and the user's application sees `EIO`/`EBADF` on file descriptors it still considers valid. Any orchestrated deployment that restarts the server (rolling updates included) hits this.

## The crux (why an external session database is the wrong tool)

One instinct is to persist sessions and open files in an external database and restore them on the new process. This does not work for the hard part:

- **The wire protocol is offset-based.** `Read`/`Write`/`WriteAndFlush` carry `offset` per-RPC (`api/proto/file.proto`); there is no server-side seek position. A server fd is essentially `(path, flags)` plus a process-bound `*os.File` handle (`FileEntry`, `pkg/server/service/session.go`).
- **A file descriptor cannot outlive its process.** You cannot serialize `*os.File` into a database and "restore" it in a new process. When the process dies, the kernel reclaims the fd. "Restore open files from the DB" can therefore only ever mean **reopen by path on the new process** — and `(path, flags)` is information the *client* already holds, because the client issued the `Open`.

So a database would store metadata the client already has, while adding a stateful dependency on the filesystem hot path (every `Open`/`Release`/lock round-tripping to it) and a new failure domain (DB down → every mount down). It pays a high price and still does not solve fd restoration.

The proven design for this problem is **NFSv4 server-reboot recovery**: the client is the source of truth for "what I have open," and on detecting a server restart it *reclaims* its state by replaying opens (and, in the full version, locks) against the new server. State authority lives at the client, not in a server-side store.

## Deployment assumption

Reopen-by-path only works if the restarted process sees the **same underlying storage** (the same volume/data directory the original process served). Two requirements on whatever supervises the server:

1. **Same data on restart.** The new process must come up with the same backing storage present and readable at the same paths.
2. **Exclusive serialized handoff.** The previous process must release the storage before the new one writes to it, so the two never have it open read-write simultaneously. The supervisor/orchestrator is responsible for this; this design assumes it and does not attempt to arbitrate concurrent writers itself.

The restart gap (old process exits → new process ready) must fit inside the client retry window (below) for reclaim to be transparent. Both requirements are generic to any deployment model; verifying that a specific orchestrated deployment satisfies them is an operational task for that deployment, not part of this OSS design.

## Design A — client reopen-on-epoch + graceful drain

Two cooperating pieces. Drain makes *planned* restarts clean (in-flight ops finish and flush before death — no torn writes); client reopen-on-epoch makes open files actually *survive* the restart (planned or crash).

### Server

1. **Boot epoch.** On startup the server generates a process-unique **epoch id** (a random nonce, regenerated every boot). It is returned on session establishment and surfaced on fd-carrying RPC replies/metadata so a client can detect "I am now talking to a different server process." A stale epoch on an fd-op is a distinct, retryable signal — not a generic error.

2. **Graceful drain on SIGTERM.** The server traps `SIGTERM` and enters a *draining* state:
   - Reject new `Open`/`Create` and new session establishment with `UNAVAILABLE` (retryable; the client already treats this as a transient, retry-within-window condition).
   - Let in-flight fd-ops run to completion and flush.
   - Exit once in-flight work drains or the shutdown deadline approaches.
   - The supervisor's shutdown grace must comfortably exceed the expected drain time.

   Drain is a complement, not a substitute for reclaim: even with a perfect drain, the new process has an empty session table, so the client must still reopen. Drain's job is to avoid losing unflushed in-flight bytes during *planned* restarts. A crash skips drain; reclaim still recovers the files (losing only the unflushed in-flight bytes from the crash).

### Client

Extend the existing resilient-mount machinery (PR `#93`: windowed `retryOp`, fresh per-attempt deadlines, `WaitForReady`, and the session-change guard). Today the session-change guard *stops* fd-ops when it detects the session changed. This design changes that response from "stop" to "reclaim":

1. **Detect epoch change.** When an fd-op observes a new epoch (or the session is unknown to the server), the client treats it as a reclaim trigger rather than a terminal error.

2. **Re-establish the session.** Open a fresh session against the new process. Do **not** reuse or persist the old `session_id`/bearer token in any shared store — re-establish credentials normally. The new session gets the new epoch.

3. **Reopen open files by `(path, flags)`.** For every file the client currently holds open, re-issue `Open` against the new session, obtain the new server-assigned fd handle, and **remap** the client's internal handle → server-fd table. The application's file descriptors are unchanged; only the server-side handle behind them is swapped.

4. **Resume.** In-flight reads/writes that were stalling inside the retry window now succeed against the reclaimed handles. The application's `read()`/`write()` calls never returned an error — they stalled and resumed.

All of this happens inside `rpc.retry_window` (default 60s). If reclaim cannot complete within the window (e.g., the server never comes back), the client surfaces the error as it does today.

### Data flow (planned restart)

```
app write() ─▶ client fd table ─▶ Write(fd=7, offset, data) ─▶ server(epoch=A)
                                                                     │ SIGTERM
                                                  UNAVAILABLE ◀──────┘ drain: finish in-flight, flush, exit
client: retryOp window, WaitForReady
                                                              ┌──── server(epoch=B) boots, empty sessions
re-establish session ───────────────────────────────────────┤
reopen path "/foo" flags=O_RDWR ─▶ Open ─▶ new fd=3 ─────────┤
remap client handle: 7 → 3                                    │
retry Write(fd=3, offset, data) ─────────────────────────────┴──▶ OK
app write() returns ───────────────────────────────────────────── (never saw an error)
```

## Error handling & edge cases

- **Crash mid-write (no drain):** reclaim recovers the open file; unflushed in-flight bytes from the crashed op are lost (same as any process crash on a local filesystem — the app's `write()` had not been `fsync`'d). Acceptable.
- **Reclaim exceeds retry window:** surface the error as today (`#93` behavior). No silent hang.
- **Unlinked-but-open files:** the one genuinely hard case — the path no longer resolves, so reopen-by-path fails for a file that was `unlink`'d while held open. Known, bounded limitation for Design A. NFS solves this with silly-rename (`.nfsXXXX`); out of scope here, documented as a limitation.
- **Byte-range locks (`SetLk`/`SetLkw`):** **not** re-asserted in Design A. When a single client holds a volume, no other client contends a lock during the restart gap; intra-client lock state is not preserved across reclaim. Correct cross-client lock recovery is Design B (below).
- **Concurrent-writer safety:** out of scope — the design assumes the supervisor enforces exclusive serialized handoff (see Deployment assumption) so the old and new process never write the same storage at once.

## Non-goals

- No external/shared session database.
- No persistence of `session_id` or any credential in shared state.
- No cross-client lock recovery (Design B).
- No recovery of unlinked-but-open files.
- No arbitration of concurrent writers — that is the supervisor's responsibility.

## Testing

- **Unit (server):** epoch generation is per-boot-unique; drain rejects new `Open`/`Create` with `UNAVAILABLE` while letting registered in-flight fd-ops complete; SIGTERM path. Testify suites per house convention.
- **Unit (client):** epoch-change detection triggers reclaim; reopen remaps the handle table; reads/writes resume on the new handle; reclaim failure past the window surfaces the error. Test the *property the fix protects* (handle remapped, op resumed), not a guessed error shape.
- **E2E (`test/e2e/`, real FUSE mount):** mount a volume, hold a file open mid-write, restart the server process in-place (same data dir), assert the application's `read()`/`write()` resume without error and data is intact. Gate any VM-only variant behind the existing env flags; CI has `/dev/fuse` so the in-process variant runs in CI.

## Design B — future work (documented, not built)

Server **epoch + grace window + full reclaim**, the NFSv4-grade end state. Adds to A: on boot the server opens a *grace window* during which it accepts only reclaim ops (reopen + **lock re-assert**) and blocks new conflicting opens/locks from *other* clients until grace expires; the client replays its byte-range locks as part of reclaim. This buys correct multi-client lock semantics across a restart. Deferred until there is concurrent multi-client access to a volume to make lock contention real. Design A's epoch + reopen machinery is the foundation B builds on — nothing is thrown away.

The only other legitimate use for a shared store in this area is **session observability** (listing active sessions across server instances) — explicitly deferred, and it would never sit on the read/write hot path.

## Roadmap note

When implemented, the durable record lands in `docs/design/` (reliability/recovery section, alongside `security-and-transport.md`), and a one-line row goes in `docs/roadmap.md`. This transient spec is pruned after consolidation.
