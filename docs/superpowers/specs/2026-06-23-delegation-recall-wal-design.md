# Delegation + Recall + WAL — design (IN PROGRESS)

**Status:** Brainstorming in progress (2026-06-23). Settled decisions below; open
questions still being worked. Resume the brainstorm from §"Open questions".
**Supersedes:** the **volume-level RWO** lease model in
[docs/design/rwo-wal.md](../../design/rwo-wal.md) — that doc's *failure model,
WAL mechanics, and replay-dedup fork (a)* still hold; its *§2 access-mode lease
model* is REPLACED by delegation + recall (below).

---

## Why we moved off volume-RWO

Volume-level RWO (one writer locks the **whole volume**, others *rejected at mount*)
is admission-time global exclusion — at odds with gMountie's "NFS-over-internet,
mount anywhere, multi-client" identity. The user explicitly rejected it.

The replacement is the model NFSv4 delegations / SMB oplocks already use to let
clients cache + batch aggressively **without locking the share**: optimistic
**delegation + recall**.

## The model (SETTLED)

- The volume is **always shared (RWX)** — no mount is ever rejected, no lockout.
- A client takes a **write-delegation rooted at a SUBTREE** (a path) — "I'm
  provably alone under here right now." While held, it may batch + defer freely
  under that subtree (Phase 2). One grant covers **all files + nested dirs
  created under the root** — new children inherit it, so create-heavy workloads
  (`npm install`, build trees) need **no per-file grant**.
- **Recall:** if another client accesses a path inside a delegated subtree, the
  server recalls — the holder synchronously flushes (Phase 2) / releases, then
  normal close-to-open resumes. The contender waits one recall round-trip.
  Coherence is preserved; the system **degrades gracefully on contention**
  instead of forbidding it.
- **Recall transport = extend the existing Subscribe channel** (today it pushes
  cache invalidations server→client). Recall is the same shape but needs an
  **ack/completion** (invalidation is fire-and-forget; recall must confirm the
  flush is durable before handoff).
- **Framing:** write-delegation is the *symmetric extension* of gMountie's
  existing coherence — today the client caches **reads** and the server
  invalidates on remote change; this applies the same machinery to **writes**.

### Granularity decision: SUBTREE (not per-file, not whole-volume)

- Per-file was rejected: a per-file delegation acquired per file = **one RTT per
  file** → no win (just moves the cost). Optimistic per-file create reintroduces
  the "succeeded-locally-then-conflicts" data-loss risk + thousands of
  server-tracked delegations.
- **The WAL is what kills the per-file RTT** (batches `create/write/close` ops,
  `O(bytes/flush_size)` RPCs not `O(files)`). The **delegation is only the
  coherence umbrella** that makes the batch safe — so it must be **coarse**.
- This is "**RWO, fixed**": exclusive over *one subtree*, enforced by
  *recall-on-contention*, not over the whole volume by *rejecting mounts*.
- Per-file delegation may return later as a refinement for isolated contention
  on individual existing files (split a subtree). Not the foundation.

### Subtree selection (SETTLED): client-driven + adaptive

- The delegated subtree = **"the smallest directory that contains the client's
  current active write set."** Client watches where its writes land, requests a
  delegation at their lowest common ancestor, and **promotes the root upward**
  as writes spread to siblings.
- Server's job = **admission/arbitration**: grant the requested root **iff it
  doesn't overlap another client's delegated subtree** (containment check); on
  overlap, recall the conflicting holder first, else refuse (client falls back
  to synchronous there).
- Acquisition must NOT cost a per-file RTT — demand-based / piggybacked, coarse.

### "Writes all over the place" (SETTLED behavior)

- **Scattered but ALONE** → the common ancestor of "everywhere" is the mount
  root → the delegation coarsens to one root-level grant → batches fine.
  **Locality is irrelevant when alone** (the common cloud case: 1 client/volume).
- **Scattered AND contended everywhere** → recalls fire constantly, the
  delegation can't stick → client **degrades to today's synchronous behavior**
  for contended regions. Correct, not a failure: you lose only the **speedup**,
  never correctness/data. Scatter doesn't hurt; *contention* does, and only ever
  costs speed.

### Concurrency target (SETTLED): MULTI-BATCHER — "build it properly"

- Two clients in **disjoint** subtrees (`/teamA/...`, `/teamB/...`) each get
  their own delegation and **both batch simultaneously**. The full "not-RWO"
  promise. (The simpler "single-batcher / optimistic-root, recall on any second
  writer" was considered and rejected — we're building the disjoint-subtree
  model from the start.) Needs the containment + carving logic server-side.

## Phasing (SETTLED)

- **Phase 1 — delegation + recall infrastructure, NO deferral.** Server
  grants/tracks subtree write-delegations + recalls them over Subscribe; client
  requests + honors recalls. `close()` still flushes durably (no WAL yet). Ships
  the hard part (recall protocol, delegation table, containment, liveness/
  timeout) as a coherence layer that **literally cannot lose data** — nothing is
  deferred. De-risks before anything is at stake. **This cycle's scope.**
- **Phase 2 — WAL + deferred `close()` on held delegations.** `close()` defers
  into the WAL; flush triggers = interval/size/fsync/recall; replay + watermark
  dedup on reconnect. Speedup *and* the bounded machine-death loss window appear
  here, on machinery already trusted.

## Data-safety model (SETTLED — the "will it eat my data" map)

1. **Always safe:** `fsync`/`fdatasync` = hard barrier, durable before return.
   Recall forces synchronous flush → **no live client ever reads stale data**
   (coherence is absolute).
2. **Lost but recovered:** client *process* dies, machine lives (pod restart) →
   boot-epoch reclaim (PR #119) replays WAL from last server-acked seq → zero
   loss. Contention while alive → recall flushes → zero loss.
3. **Genuinely lost — one scenario only:** client *machine* dies (power/disk
   loss) with un-`fsync`'d WAL → that bounded window is gone. **Identical to a
   local FS losing dirty page cache on power loss.** Target workloads
   (node_modules, build trees, scratch, checkpoints) regenerate it.

### Two implementation landmines (MUST nail or it eats data in nastier ways)

1. **Replay dedup** — reconnect replaying an applied-but-unacked op without dedup
   = doubled append / re-run rename = corruption. Fix = **durable server-side
   seq-watermark per `(identity, volume)`** (rwo-wal.md fork (a)); replay of any
   op ≤ watermark is a no-op. SQL store only if it benchmarks on the hot path.
   Non-negotiable for Phase 2.
2. **Recall-before-handoff** — server must NOT let a contender touch a path until
   the holder's recall flush is **durably complete**. Sloppy race = stale read /
   clobbered write. Handoff invariant must be airtight.

---

## Recall rule (SETTLED): one principle

**A write-delegation is recalled the moment continuing to hold it would let
another client observe incoherent state.** One test, applied per case — no
separate read/write rules:

- **Remote WRITE → always recalls** (both phases). A second writer invalidates
  the holder's "I'm provably alone here" assumption regardless of deferral.
- **Remote READ → recalls only if the holder might have unflushed data.**
  - *Phase 2 (WAL):* YES. The holder may have deferred creates/writes in its WAL;
    the reader would miss them. Recall forces a durable flush, then the read
    proceeds. **"Read" includes `readdir`/`lookup`, not just file reads** — the
    coherence-critical contender op for the target workloads is a *directory*
    read (client B `readdir /dir` while A streams `npm install` creates into its
    WAL); that readdir must recall or B sees a half-populated dir.
  - *Phase 1 (no deferral):* NO. `close()` already flushed, so the reader gets
    normal close-to-open consistency — no server-invisible dirty state to miss.
    The holder keeps its delegation; it loses skip-revalidation only on a remote
    *write*.

Consequence for Phase 1's value prop: with no deferral a "write delegation" is
functionally a **read/coherence delegation** — its only user-visible payoff is
**skip-revalidation** (recalled on remote write). The write-side batching speedup
is **entirely Phase 2**. (Resolves open-Q #2: Phase 1's observable win is
read-side.)

## Contention handoff (SETTLED): server-mediated, never a unilateral client choice

"Fall back to the slow path" is **never** the contender's decision for a
delegated region. The server's delegation table is authoritative:

1. Contender B's read/write hits the **server**.
2. Server sees the path under A's delegation → **recalls A**, *blocks B's op*
   until A's flush is **durably complete + acked** (landmine #2).
3. Server serves B. The contended region is now delegation-free → A and B both
   run **synchronously** there → coherent.

B's stall = **recall RTT + the holder's flush duration** (a large WAL isn't free;
don't round to "one RTT"). In the common 1-client/volume case it never fires.

## Re-acquisition + thrash prevention (SETTLED): server-side hysteresis

Lost delegations are **not** restored automatically — the client **re-pulls**
them with the same demand-based request, **piggybacked** on an op it's already
sending (no extra RTT). The "B writes once then idles" case: B left no delegation
behind (during contention both run delegation-free), so A's next write carries a
re-request flag, the server's containment check passes, and A is back on the fast
path — B going *idle* isn't the trigger, B *holding nothing* is.

Thrash prevention is **server-side, not client backoff** — correctness must not
depend on a client choosing to behave (a buggy/hostile client that re-requests
every op must not be able to make the server thrash):

- **Cooldown table.** On recall, the server marks the contended root/sub-path
  *recently-contended* with an `until` timestamp. Re-grant requests overlapping a
  cooling path within the window are **denied** (client stays synchronous).
  TTL'd, bounded, separate from the delegation table.
- **Exponential + capped.** Repeated recalls on the same root extend its
  cooldown → a true ping-pong region gets stickier-synchronous and the server
  stops paying recalls; a one-off collision has near-zero cooldown so A re-grants
  on its very next request.
- **Grant may be narrower than requested.** Client asks for `/dir`; server grants
  `/dir` *minus* cooling sub-paths and returns the actual granted root + excluded
  paths. **All carving policy is server-side**; the client honors what it's told.
  This is carve-around-the-hotspot, server-decided. (Resolves most of open-Q #1.)
- **Client spam is harmless.** Re-request is a piggybacked bool; the server
  answers "denied, retry-after N" in the response it was already sending.
  Correctness never depends on the client honoring `retry-after`; rate-limit the
  containment check only if it ever matters.

Arbitration loop: **client pulls (piggybacked) → server checks containment +
cooldown → grants the largest non-cooling sub-root, or denies → recall on
contention restarts the cooldown.** Delegation table + cooldown table together
are the single source of truth; the client is purely advisory.

---

## Open questions (RESUME THE BRAINSTORM HERE)

1. **Multi-batcher containment + carving — data structures.** The *policy* is
   settled (server-side cooldown, narrower-than-requested grants, carve around
   cooling paths). Open = the concrete server-side representation: how the
   delegation table + cooldown table are indexed for the overlap/containment
   check on every (piggybacked) request without it becoming a hot-path cost.
   Path-prefix tree? Interval/trie? Decide at plan time.
2. ~~What Phase 1 delivers observably~~ — **RESOLVED**: read-side
   skip-revalidation (recalled on remote write); write-batching is Phase 2. Test
   via RTT-count assertions (revalidation suppressed while delegated).
3. **Mount-time negotiation surface.** Where does "I want delegations" live — a
   mount flag, a field on WhoAmI / an existing RPC, or a new lease/delegation
   RPC? Likely a proto change; touches SingleVolumeMounter.
4. **Recall wire protocol.** Extend Subscribe: recall message + ack/completion +
   timeout; liveness reusing grace-period reap + boot-epoch reclaim, scoped
   per-delegation.
5. **Delegation acquisition cost / piggybacking** — confirm no per-file RTT;
   demand-based promotion mechanics.
6. **SQL latency benchmark** for the watermark (Phase 2, fork (a)) — measure
   before committing to SQL on the hot path.
7. **Layer-stack placement** — the per-fd handle-layer seam
   ([client-architecture.md](../../design/client-architecture.md) §9) is a
   Phase 2 prerequisite for where the WAL sits.
8. **ROX (read-only-many)** — later freebie, out of scope.

## Process state

Mid `superpowers:brainstorming`. Next steps when resumed: finish the open
questions (esp. #1, #2, #3 for Phase 1), present the full Phase-1 design for
approval, finalize this spec, then `superpowers:writing-plans`. Feature lives in
worktree `worktree-delegation-wal`; design + implementation ship in one PR
(consolidate-related-PRs). Fold this superpowers spec into docs/design/ when the
feature ships.
