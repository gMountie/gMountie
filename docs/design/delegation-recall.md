# Write Delegation and Recall

**Status:** Phase 1 (delegation + recall coherence layer) shipped (2026-06-23). Phase 2 (WAL + deferred close) deferred.
**Last updated:** 2026-06-23

This document covers gMountie's write-delegation and recall system: how clients
can hold a write-delegation over a subtree, how the server arbitrates grants and
forces a coherent handoff on contention, and how Phase 2 (the WAL) will extend
that machinery to batch writes across files. The access-mode lease model
previously recorded in [rwo-wal.md](rwo-wal.md) §2 is superseded by this
document; that doc's failure model and WAL mechanics still apply in Phase 2.

---

## 1. The problem

Small-file write workloads over WAN are round-trip-bound: `npm install`, build
trees, `git checkout` — thousands of `open → write → close` sequences, each
serialized at 60–150 ms per file. The coalescer already batches bytes within one
file; deferring `close()` across files requires the client to be provably alone
in the region it is writing to, so that deferred data cannot be observed by
another client in an inconsistent state.

Phase 1 ships the **coherence layer** that makes that isolation provable, with
`close()` still flushing durably (no deferral). Phase 2 adds the WAL that turns
the isolation into a write-speed win.

---

## 2. The model

- **The volume is always shared (RWX).** No mount is ever rejected; no client is
  locked out.
- **A client takes a write-delegation rooted at a subtree** — "I am provably
  alone under this path right now." While held, it may (eventually) batch and
  defer freely under that subtree. A single grant covers all files and nested
  directories created under the root: new children inherit it, so create-heavy
  workloads need no per-file grant.
- **Recall.** When another client accesses a path inside a delegated subtree the
  server recalls the holder: the holder synchronously flushes (Phase 2) or
  releases, and normal close-to-open consistency resumes. The contender waits one
  recall round-trip. The system **degrades gracefully on contention** — never
  forbids it, never loses data.
- **Framing.** Write-delegation is the symmetric extension of the existing
  coherence model: today the client caches reads and the server invalidates on
  remote change; this applies the same machinery to writes.

### 2.1 Granularity: subtree, not per-file and not whole-volume

Per-file delegation — one grant per file, acquired on open — costs one RTT per
file. The delegation is only the coherence umbrella that makes batching safe; the
WAL is what eliminates the per-file RTT. So the delegation must be coarse.

Volume-level exclusion (one writer locks the whole volume) conflicts with
gMountie's "mount anywhere, multi-client" identity and was explicitly rejected.

Subtree granularity gives two clients writing to disjoint subtrees
(`/teamA/…`, `/teamB/…`) independent delegations, batching simultaneously.

### 2.2 Subtree selection: client-driven, server-arbitrated

The delegated root is the lowest common ancestor of the client's active write
set. The client watches where writes land, requests a delegation at that LCA, and
promotes the root upward as writes spread to siblings. The server's role is
arbitration: grant the requested root if and only if it does not overlap another
client's delegation or a cooling subtree; otherwise recall the conflicting holder
first, or deny (client falls back to synchronous close-to-open).

Acquisition is demand-driven and piggybacked on an operation the client is
already sending — no extra RTT, no mount-time capability handshake.

### 2.3 "Writes scattered across the whole volume"

**Alone but scattered:** the LCA of "everywhere" is the mount root. The
delegation coarsens to a single root-level grant and batching works fine.
Locality is irrelevant when alone — the common cloud case (one client, one
volume).

**Contended everywhere:** recalls fire constantly; the delegation cannot stick;
the client degrades to today's synchronous behavior for the contended regions.
The scatter costs only speed, never correctness or data.

---

## 3. The one-principle recall rule

**A write-delegation is recalled the moment continuing to hold it would allow
another client to observe incoherent state.** One test, applied per case:

- **Remote write → always recalls (both phases).** A second writer invalidates
  the "provably alone" assumption regardless of deferral.
- **Remote read → recalls only if the holder might have unflushed data.**
  - *Phase 2 (WAL):* yes. The holder may have deferred creates or writes in its
    WAL; a reader (including `readdir`) would miss them. Recall forces a durable
    flush before the read proceeds.
  - *Phase 1 (no deferral):* no. `close()` already flushed; the reader gets
    normal close-to-open consistency. The holder keeps its delegation and only
    loses skip-revalidation on a remote write.

---

## 4. Server-mediated handoff

"Fall back to synchronous" is never the contender's unilateral decision for a
delegated region. The server's delegation table is authoritative:

1. Contender B's read or write hits the server.
2. The server sees the path under A's delegation and recalls A, **blocking B's
   operation** until A's flush is durably complete and acked.
3. The server serves B. The contended region is now delegation-free; both clients
   run synchronously there.

The barrier (step 2) is enforced at the server's per-region **arbitration lock**,
not by stream or message ordering. With recall running on a dedicated bidi stream,
A's flush, B's contending op, and the recall acknowledgment ride different
connections; the lock is the only reliable serialization point.

In Phase 1 there is no WAL flush, so the stall B observes is just the recall
RTT plus A's cache-invalidation time — a small, bounded cost.

---

## 5. Server-side hysteresis

### 5.1 Cooldown table

On every recall the server marks the contended root or sub-path as
*recently-contended* with an `until` timestamp. Re-grant requests that overlap
a cooling path within the window are denied; the client stays synchronous.

- **Exponential + capped.** Repeated recalls on the same root extend its
  cooldown; a genuine ping-pong region gets stickier-synchronous and the server
  stops paying recalls. A one-off collision has near-zero cooldown, so A
  re-grants on its very next request.
- **Client spam is harmless.** The re-request is a piggybacked bool on an
  existing op. The server answers "denied, retry-after N" in the response it was
  already sending. Correctness never depends on the client honoring `retry-after`.

### 5.2 Narrower-than-requested grants

The client asks for `/dir`; the server grants `/dir` minus cooling sub-paths and
returns the actual granted root plus excluded paths. All carving policy is
server-side; the client honors what it receives.

### 5.3 Re-acquisition

Lost delegations are not restored automatically. The client re-pulls with the
same demand-based piggybacked request. When the contender goes idle without
acquiring a delegation, the arbiter's containment check passes on A's next write
and A is back on the fast path immediately.

---

## 6. Phase 1 scope (shipped)

Phase 1 ships the full coherence layer — grant, track, arbitrate, recall — with
`close()` still flushing durably. Nothing is deferred, so this phase **cannot
lose data**. The feature is purely infrastructural.

### 6.1 Honest value framing

**Steady-state read performance is already delivered by the verified cache.** Once
the Subscribe stream delivers its first HEARTBEAT, the `validityTracker` flips to
Verified and reads are served with no server round-trip (within TTL). Phase 1
does not change this. Three wins that Phase 1 does deliver:

1. **Protocol de-risk.** The recall bidi stream, arbiter, and delegation table
   are exercised end-to-end now — with no data at stake — so Phase 2 WAL builds
   on trusted machinery.
2. **Provable coherence for delegated subtrees.** The lock-enforced
   recall→invalidate→ack→handoff barrier closes the known memory-tier stale-read
   race for delegated subtrees. A remote writer is recalled before a delegated
   client's read could return stale data.
3. **Reconnect-window perf bonus.** While delegated, the client may skip
   `GetAttrIfChanged` even during Unverified windows (Subscribe stream down or
   reconnecting). On a flaky link, Unverified windows are more frequent, so the
   bonus scales with link flakiness. On a reliable link in steady state, the
   verified cache already covers this — the bonus is small.

**Write-side batching speedup is entirely Phase 2.** Phase 1 does not defer
closes; there is nothing to batch.

### 6.2 Components

**Server — `pkg/server/delegation/`** (all in-memory soft state):

- **Delegation table** — delegated-root → `{session, identity, gen}`; path-trie
  indexed for containment and prefix lookup. Non-durable: rebuilt from client
  re-requests on server restart.
- **Cooldown table** — recently-recalled root → `until`; TTL'd and LRU-capped.
- **Arbiter** — single authority: containment check vs delegation + cooldown
  tables → grant largest non-cooling sub-root, or deny. Holds the per-region
  arbitration lock that enforces the recall→flush→handoff barrier.
- **Recall registry** — coordinates in-flight recalls; coalesces concurrent
  recalls for the same region so at most one recall is in flight per region, and
  late contenders queue on the in-flight handoff.

Injected via `AppContext`; wired into the `ReleaseSession` reap hook so
delegations are dropped when a session expires.

**Wire — `RpcFs.Recall` bidi stream:**

- Delegation request = a field piggybacked on existing op RPCs; response carries
  `{granted_root, excluded_paths, retry_after}`.
- Recall = a dedicated server→client bidi stream (not the Subscribe channel —
  recall needs ordered ack/completion that fire-and-forget invalidation lacks):
  `Recall{root}` → `RecallAck{root, done}`.

**Client — `pkg/client/backend/delegation/`:**

- **Manager + delegation layer** at the reserved `posWritePath` slot in the
  backend chain.
- **Active-write-set tracker** — computes the LCA subtree to request, promotes
  upward as writes spread, piggybacks the request on outgoing ops via a
  `DelegationHook`.
- **Recall handler** — on `Recall`: invalidate read cache for affected paths,
  then drop the delegation and skip-revalidation gate, then ack. Invalidating
  before dropping is a correctness requirement: dropping without flushing the
  cache is a stale-read bug.
- **Cache `IsDelegated` oracle** — the cache layer consults this to decide
  whether to suppress `GetAttrIfChanged` during Unverified windows.

### 6.3 Corner cases (Phase 1)

| Case | Handling |
|---|---|
| Recall timeout | Server drops the delegation (no deferred data to lose); holder loses skip-revalidation and resumes revalidating |
| Cross-subtree rename/link | Forced synchronous; arbitrated against all involved subtrees; path lock-ordering (canonical order) prevents deadlock |
| Cache-invalidate on recall | Holder MUST invalidate read cache for affected paths before sending RecallAck — drop-without-invalidate is a stale-read bug |
| Recall coalescing | One recall per region at a time; additional contenders queue on the in-flight handoff |
| Self-access | Never recalls its own delegation; cooldown only triggers on a recall event |

---

## 7. Phase 2 gates (deferred — not shipped)

These items must be designed before deferral ships. None are in scope today.

**WAL + deferred `close()`** — the actual write-side speedup. `close()` defers
into an on-disk WAL; flush triggers are interval/size/`fsync`/recall; WAL replay
with watermark dedup runs on reconnect.

**Fencing by delegation generation (gate #1 — the boot-epoch hole).** PR #119's
boot-epoch reclaim is correct today because Phase 1 does not defer. Once WAL
replay exists, the hole opens: holder A's machine dies; grace expires; server
hands the region to B; A reboots with a new boot epoch; #119 reclaim replays A's
WAL; A's ops are `> watermark` (never acked) and are not deduped by the
seq-watermark; they clobber B. Boot-epoch reclaim is the hole, not the fix.

Fix: fence by **delegation generation**, not boot epoch. When the server revokes
A's delegation and hands the region to B, it durably records the revoked
delegation-gen. WAL replay must present a still-live gen; any op tagged with a
revoked gen is rejected regardless of its seq. The gen-revocation record and the
seq-watermark must be **one durable store** — if they diverge, A's watermark
advancement by its previous cycle never fences A's superseded replay.

Run an **SQL latency benchmark** for the watermark/revocation store before
committing SQL on the hot (per-op) path.

**Per-fd handle-layer seam** — the WAL sits at a seam inside the per-fd layer,
described in [client-architecture.md](client-architecture.md) §9. This seam is a
prerequisite for correct WAL placement.

**Failure model during Phase 2:**

- `fsync`/`fdatasync` = hard barrier; ops are flushed durably before return.
- Deferred-op failure (ENOSPC/EIO) on replay or recall-flush aborts the handoff:
  the server must not serve the contender until the holder's flush is confirmed
  durable.
- Identity-bound replay: WAL ops are replayed under the holder's bound identity;
  identity revocation discards un-replayed WAL, consistent with the session-death
  model.
- **Data-loss window** = machine death or a network partition that outlasts the
  grace period (the server cannot distinguish a dead client from a partitioned
  one). Un-`fsync`'d WAL data in that window is lost. This is identical to a
  local filesystem losing dirty page cache on power failure, and to NFS/SMB lease
  expiry. Target workloads (build trees, `node_modules`, scratch, checkpoints)
  regenerate the data.

---

## 8. Data-safety summary

| Scenario | Phase 1 | Phase 2 |
|---|---|---|
| `fsync`/`fdatasync` | Durable before return, always | Durable before return, always |
| Remote contention | Recall flushes → coherent, no loss | Recall flushes WAL → coherent, no loss |
| Client process dies, machine lives | No deferred data; close already durable | Boot-epoch reclaim + gen-fence replays WAL, zero loss |
| Machine death or partition > grace | No deferred data; no loss | Bounded dirty WAL window lost — same as local-FS power loss |

---

## 9. What this intentionally does not do

- **Volume-level exclusion (RWO).** Rejected: a single writer locking the whole
  volume conflicts with the "mount anywhere, multi-client" identity. The
  delegation + recall model is "RWO, fixed": exclusive over one subtree, enforced
  by recall on contention.
- **Per-file delegation.** One grant per file = one RTT per file on acquisition;
  the RTT problem is not solved. Per-file delegation may return as a refinement
  for isolated contention on individual existing files.
- **Read-only-many (ROX) leases.** Out of scope; a later addition once the write
  delegation machinery is proven.
- **Mount-time negotiation.** No capability handshake. The client piggybacks
  delegation requests; a server that does not support them ignores the field.
