# Resilient mount retry — survive transient network outages

**Status:** design approved, pre-implementation
**Date:** 2026-06-09
**Repo:** public `gMountie/` (OSS client + a small server-config addition) — additive, WAN-resilience; no cloud-specific hooks.

## Problem

`gmountie mount` proxies every FUSE syscall to the server as a gRPC call, each
bounded by a fixed per-RPC deadline (`timeout_meta` = 5s metadata, `timeout_io`
= 30s data) and retried ≤3 times. Over a slow or intermittently-dropping
internet link — gMountie's primary use case — a single metadata op can exceed
5s; when it does, `BackendClient.Stat` (and siblings) return `fuse.EIO` and the
userspace app (e.g. `cp` of a large file) **aborts**.

Two defects compound it:

1. **Shared retry budget.** The per-op deadline context is created *once,
   outside* `retryableCall` (`backend_grpc.go` `metaCtx`/`ioCtx`), then passed
   into the retry loop. All three attempts share one 5s budget, so once the
   first attempt exhausts it the context is done and `retry-go` aborts. The
   "retry on `DeadlineExceeded`" is effectively dead on a slow link.
2. **No outage tolerance.** A hiccup longer than one deadline surfaces as a hard
   `EIO`, even though the session layer (`SessionHandshake`) already
   auto-recovers (Resume→Create) once its keepalive stream breaks.

Observed: copying `*.mkv` over a throttled link produced a wall of
`code = DeadlineExceeded` on `Access`/`GetAttr`/`GetXAttr`/`Write`, then the
copy failed. Raising the timeouts via config made it succeed — confirming the
deadline was the binding constraint.

## Goal

A transient failure should **pause and resume** the operation rather than abort
it: retry through an outage for a bounded, configurable **retry window**; only
when the window elapses does the error reach userspace, so a genuinely dead
server still unblocks apps instead of hanging forever.

## How recovery actually works (the constraint that shapes this design)

The server keeps per-client session state — fd table **and** the
`(session_id, request_id)` idempotency cache — on the `Session` object
(`service/session.go`). On a keepalive-stream break the client recovers:

- **Resume** (`SessionManager.Resume`) reattaches to the **same** session if it
  still exists: same `session_id`, **fds and idempotency cache intact**. Safe to
  retry anything.
- **Create-fallback** happens only when the server-side session is **gone** —
  reaped after the disconnect **grace period**, evicted by revocation
  (`ReapIf`), or lost to a server restart. The new session has an **empty cache**
  and **no fds**. The client even logs this: *"session re-created after resume
  failure (open fds are now invalid)"* (`session.go:220`); fd ops then return
  `EBADF` / `NotFound "fd N not found"` (`controller/file.go`).

**Therefore transparent resume is bounded by the server grace period, not the
client window.** A drop **under** grace → Resume → fds+cache intact → any op
resumes. A drop **past** grace → Create → fds dead, cache empty → an in-flight
copy cannot transparently continue, and a replayed *path mutation* could surface
a spurious `EEXIST`/`ENOENT`.

Grace is currently **hardcoded to 30s** (`DefaultGracePeriod`; `app.go:65`
constructs the manager without setting it) and **not configurable**.

## Decisions (locked during brainstorming)

| Decision | Choice |
|---|---|
| Outage semantics | Bounded grace window, configurable. `0` = today's fail-fast. |
| Default client window | **60s** (soft). |
| Approach | **A + C**: time-bounded retry loop with fresh per-attempt deadlines, plus gRPC `WaitForReady` on retried FS calls. (Explicit health-gated pause, "B", rejected.) |
| Window vs grace | **Align them.** Make server grace configurable, default **60s** to match the client window, so a drop up to ~60s actually Resumes. |
| Mutation safety | Do **not** retry path-mutations across a session-id change (Create). |
| Interruptibility | Op stays interruptible by unmount/Close (lifetime ctx). Preserve the existing `WithoutCancel` stance toward the FUSE-op ctx (keeps the async-preemption `EIO` fix). |

## Design

### 1. Client config — `pkg/client/config/rpc.go`

Add `RetryWindow time.Duration` (`mapstructure:"retry_window" validate:"gte=0"`),
`DefaultRpcRetryWindow = 60s`, wired in `NewRpcConfig` defaults. `timeout_meta`
(5s) / `timeout_io` (30s) keep their values; doc comments clarify they now bound
a **single attempt**. Thread `RetryWindow` onto the client (option + getter,
mirroring `WithTimeouts`).

### 2. Retry core — `pkg/client/io/retry.go`

Rework `retryableCall` from "N attempts sharing one deadline" to a wall-clock
window loop:

1. `window <= 0` → single attempt bounded by the per-attempt timeout (preserves
   fail-fast).
2. Else `outer, cancel := context.WithTimeout(lifeCtx, window)` — bounded by the
   window **and** cancelled by unmount/Close.
3. Loop while `outer.Err() == nil`: build a **fresh** per-attempt context
   (`timeout_meta`/`timeout_io`) each iteration (kills the shared-budget bug),
   carrying the caller's identity (see context-merge note), call `fn` (with
   `WaitForReady`, §4). Success → return. Retryable (`Unavailable`/
   `DeadlineExceeded`) → back off (existing cadence, clamped to remaining budget)
   and continue, subject to the **session-change guard** (§3). Permanent → return
   immediately.
4. Window expired → return the last error (mapped to errno).

Remove the fixed `retryAttempts = 3`; the window is the bound.

**Context-merge note (plan):** Go has no native "values from A, cancel from B".
Prefer extracting `*proto.Caller` via `callerFromCtx` once before the loop and
passing it explicitly into `fn` — drops reliance on ctx-value propagation across
retries and makes each attempt self-contained.

### 3. Session-change safety (the mutation-replay + dead-fd guard)

Capture `startID := client.SessionID()` at op start. Before each **retry**,
compare to `client.SessionID()`. If it **changed** (a Create-fallback happened),
behaviour depends on op class:

| Op class | On session-id change |
|---|---|
| Idempotent path reads (`GetAttr`, `Access`, `*XAttr` get, `OpenDir`, `Readlink`, `StatFs`) | **may continue** retrying — safe, and may succeed on the new session |
| fd-based ops (`Read`, `Write`, `Flush`, `Fsync`, `Release`, `Allocate`, locks) | **stop** — the fd is dead (`EBADF`); retrying is futile. Return the error. |
| Path mutations (`Mkdir`, `Rmdir`, `Rename`, `Symlink`, `Link`, `Unlink`, `Chmod`, `Chown`, `Utimens`, `SetXAttr`, path `Truncate`) | **stop** — the new session's cache is empty, so a replay risks a spurious `EEXIST`/`ENOENT`. Return the error. |

Same-session retries (Resume kept the id, or no recovery happened) are always
safe — the server dedups mutating ops via the intact `(session_id, request_id)`
cache, and writes are position-stable. The op classification is passed into
`retryableCall` (e.g. an enum/flag per call site) so the loop knows which rule to
apply.

### 4. Wait-for-ready — the "C"

`grpc.WaitForReady(true)` as a per-call option on the **retried FS RPCs only**:
during a connection `TRANSIENT_FAILURE`/reconnect the call blocks (up to its
per-attempt deadline) instead of instantly returning `Unavailable`. **Not** on
the handshake / keepalive / pre-session Resolve — those must fail fast to
*trigger* `SessionHandshake.recover()`.

### 5. Server grace — make it configurable and default it to 60s

- Add a server config field (`server.session.grace_period`, `GMOUNTIE_`-prefixed,
  `validate:"gte=1s"`) parsed through `pkg/common/config`.
- Wire it at `app.go:65`: `NewSessionManager(SessionManagerOptions{Metrics: m,
  GracePeriod: cfg.Server.Session.GracePeriod})`.
- **Default 60s** (was an implicit 30s) to match the client window default, so
  OSS defaults are self-consistent. Document the cost: the server holds a
  dropped client's fds **and POSIX locks** for the grace duration before
  releasing them — a longer grace delays lock release for other clients.

### 6. Interaction / correctness requirements

- **Per-call session-id injection:** the client metadata interceptor must read
  `SessionID()` at send time, so a retried attempt after Resume/Create uses the
  current id. Verify in the plan.
- **Window ≥ recovery time:** 60s comfortably exceeds `recoveryMaxBackoff` (5s),
  so a normal reconnect completes within one window.
- **No regression of the async-preemption fix:** the FUSE-op ctx still doesn't
  cancel the RPC; only the lifetime ctx does.

## Error handling

| Situation | Result |
|---|---|
| Transient, window has time, same session | back off, retry with fresh deadline |
| Transient, session-id changed, idempotent read | may continue retrying |
| Transient, session-id changed, fd-op or path-mutation | stop, return error |
| Permanent (NotFound, PermissionDenied, …) | return immediately |
| Window elapsed | return last error → `fuse.EIO` |
| Lifetime ctx cancelled (unmount/Close) | return promptly |

## Testing

Test the **properties** protected, not exact gRPC error shapes (mocks lie about
retry-go/grpc wrapping). testify suites.

**Unit (`pkg/client/io`):**
- Transient-then-OK fake: succeeds after K failures within the window; assert
  **multiple attempts within one window** (the shared-budget regression guard).
- Permanent error → returns immediately, no retry.
- Window expiry → returns after the window, attempt count > 1.
- Lifetime cancel mid-wait → returns promptly.
- `window == 0` → exactly one attempt.
- **Session-change classification:** with a fake whose `SessionID()` flips
  mid-op, assert a path-mutation **stops** (no further attempts), an fd-op
  **stops**, and an idempotent read **continues**.
- Injected clock / short windows for speed.

**Integration (bufconn):**
- Drop **shorter** than grace → Resume → a `GetAttr`/`Read`/`Write` straddling
  it completes.
- Drop **longer** than grace (force the **Create** path — evict the session /
  exceed grace): an fd-op fails cleanly (`EBADF`); a path-mutation retried across
  it is **not double-applied** and does not surface a spurious `EEXIST`/`ENOENT`.

**Manual / perf (documented, not CI-gated):**
- `scripts/start-slow-loopback.sh` (tc netem): reproduce the original large-file
  copy and confirm it survives. **Also run one netem case with a concurrent
  large write** to exercise the contention scenario (metadata sharing the link
  with bulk write). If metadata still starves within the per-attempt deadline,
  the composing lever is raising `timeout_meta` — the window alone won't beat
  sustained saturation. `stop-slow-loopback.sh` after.

## Files touched

- `pkg/client/config/rpc.go` — `RetryWindow` field + default + doc tweaks.
- `pkg/client/io/retry.go` — window loop; op-class guard; remove fixed cap.
- `pkg/client/io/backend_grpc.go` — call-site rewiring; `WaitForReady`;
  lifetime-vs-values ctx; per-call op classification.
- `pkg/client/grpc/client.go` / `factory.go` — carry `RetryWindow` (option +
  getter).
- `pkg/server/config/...` + `pkg/server/app.go` — server `grace_period` config +
  wiring + 60s default.
- Tests alongside; client/server config reference docs.

## Cloud follow-up (separate, tracked in gMountie-cloud roadmap)

The cloud data-plane runs the OSS server image and will **inherit** the new 60s
grace default. Pin it **explicitly** in the data-plane chart/config to 60s so the
window↔grace alignment isn't silently coupled to the OSS default, and confirm
the resource cost (held fds/locks for 60s) is acceptable on the shared
data-plane. One roadmap row in `gMountie-cloud`.

## Open implementation questions (for the plan, not blockers)

- Exact context-merge mechanism (prefer extracting `*proto.Caller` up front).
- Where op classification lives (per-call-site constant vs. derived from method
  name) — prefer an explicit per-call argument for clarity.
- Backoff clamp as the window nears expiry (clamp final sleep to remaining
  budget).
