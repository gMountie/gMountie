# Write Delegation and Recall

**Status:** Phase 1 (delegation + recall coherence layer) shipped (2026-06-23). Phase 2 (WAL + deferred writes) shipped (2026-06-24).
**Last updated:** 2026-06-24

This document covers gMountie's write-delegation and recall system: how clients
can hold a write-delegation over a subtree, how the server arbitrates grants and
forces a coherent handoff on contention, and how Phase 2 (the WAL) extends that
machinery to batch writes across files. The access-mode lease model previously
recorded in [rwo-wal.md](rwo-wal.md) §2 is superseded by this document.

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
RTT plus A's cache-invalidation time — a small, bounded cost. In Phase 2, the
stall extends to include the WAL prefix flush.

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

- **Manager + delegation layer** at the `posWritePath` slot in the backend chain.
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

## 7. Phase 2 (shipped): WAL + write batching

Phase 2 adds the on-disk Write-Ahead Log that converts Phase 1's provable
isolation into a write-speed win. Under a held delegation, mutating ops defer
into the WAL and flush in pipelined batches via a single `Apply` streaming RPC,
collapsing the per-file `open→write→close` RTT that dominates small-file WAN
workloads.

### 7.1 The overlay model — WAL + pending overlay (read-your-own-writes)

Two co-managed structures, kept consistent in one step on every deferred op:

- **WAL** (`pkg/client/backend/wal/`) — the on-disk, ordered, sequence-numbered
  op-log per `(identity, volume)`. The **single source of truth for un-flushed
  state.** Backed by bbolt (already a dependency via the cache persist tier).
  Survives client-process restart.
- **Pending overlay** — an in-memory, `memfs`-shaped tree (path→pending node,
  **including tombstones** for deferred `unlink`/`rename`), rebuilt by scanning
  the WAL on startup.

Reads are served as **`server-acked base ⊕ pending overlay`**. The existing
two-tier cache accelerates the *base*; it never holds the only copy of pending
state — un-flushed state is never only in the evictable cache (this avoids the
dual-write / eviction hole). Byte-range reads of a partially-flushed file overlay
pending bytes over base bytes.

**This is correct precisely because of the delegation.** The client is provably
alone under the subtree, so the local view (base ⊕ pending) is authoritative; no
remote writer can change it without a recall first.

The WAL+overlay layer sits **outer of the cache** in the backend chain (a new
`posWAL` slot between `posObserver` and `posCache`). The cache holds only the
`base`; the overlay is the single source of truth for un-flushed state. The cache
has no write-through obligation toward pending state.

### 7.2 Handle-layer seam

Per-fd write state (`WriteCoalescer`) was lifted off the transport's
`grpcFileHandle` into a composable per-fd **`walHandle`** at the `posWritePath`
slot. The `walHandle` wraps the transport handle, participates in the `Unwrap()`
walk (so the transport resolves the leaf fd), and provides a drain seam the WAL
can target.

Two complementary tiers remain:
- **Coalescer** batches *bytes within one file*.
- **WAL** batches *ops across files*.

On the **delegated path**, the coalescer's drain target becomes "append a
write-op to the WAL" instead of `WriteAndFlush`-to-wire; `close`/`create`/
`rename`/`unlink`/metadata likewise defer into the WAL and update the overlay.
On the **un-delegated path** (no active delegation, or after recall/denial), the
coalescer drains synchronously via `WriteAndFlush` as today — the WAL activates
only when `IsDelegated` is true.

### 7.3 Sequence, generation, and replay (corruption-critical core)

Three durable concepts:

- **`seq`** — client-assigned, monotone per `(identity, volume)` (one seq space
  even across two disjoint delegations on the volume). Orders the WAL; drives
  dedup. Carried in the `WalOp` envelope.
- **`gen`** — the server stamps a generation on each granted delegation; the
  grant returns it. The client tags every WAL op with the gen it was deferred
  under. This is the fence.
- **Watermark** — server-durable per `(identity, volume)`: the highest seq
  durably applied. Replay of any op `seq ≤ watermark` is a no-op (dedup).
  Stored **alongside the revoked-gen set in one store** — if they diverge,
  fencing breaks.

**`WatermarkStore` interface + bbolt default (`pkg/server/watermark/`):** stores
`{watermark, revoked_gens}` per `(identity, volume)`. The interface is injected
via the `server.New(cfg, ...Option)` extensibility seam (`WithWatermarkStore`).
The default impl is embedded bbolt (self-contained OSS binary; file on the
server's local disk or the cloud's persistent volume; restart-safe). The cloud
can inject a centralized (e.g. Postgres) impl through the same Option without
forking; single-node is sufficient today.

**Flush = a pipelined batch with one commit point.** A flush is a
**client-streaming RPC**: `RpcFs.Apply(stream WalOp) returns (ApplyAck)`. The
`WalOp` message is a `oneof` over the existing mutating request messages
(Create/Write/Mkdir/Rmdir/Rename/Unlink/Symlink/SetAttr/SetXAttr/RemoveXAttr
plus path-based WriteOp and ReleaseOp) **with `seq` and `gen` added**. The client
fire-hoses the batch, then half-closes; the server:

1. Applies each op **in seq order**, dispatching to a shared internal `applyOp`
   that the unary handlers also call (no logic duplication).
2. Per op: `seq ≤ watermark` → skip (dedup); `gen` revoked → reject (fenced);
   else apply + advance the in-memory watermark.
3. At stream end: **persist the watermark to the store, then return
   `ApplyAck{watermark}`**.

The **persist-before-ack invariant**: the client only drops WAL entries ≤ the
*acked* watermark; the server only acks what it durably recorded. **One RTT for
the whole batch** — the WAN win — with no per-op durable write.

**Replay on reconnect:** when a session resumes, the client re-streams its WAL
via `Apply`. Correctness and dedup come from the server's durable seq-watermark;
the server skips ops it has already applied regardless of which seq the client
starts from.

**Error mid-batch (ordered halt):** the WAL is one ordered seq-space, so the
server halts at first failure. `ApplyAck` carries `{committed_watermark,
failed_seq, fserr}`. Permanent failures (ENOSPC/EACCES/EIO) cause the client to
discard the overlay, mark the subtree EIO, and release the delegation. Transient
(EAGAIN) → retry the batch from `committed_watermark + 1`.

**Generation lifecycle + GC:** the server durably records a revoked gen in the
`WatermarkStore` *before* serving the contender on handoff. Revoked-gens are
bounded: a gen may be GC'd once `(identity, volume)`'s watermark passes the gen's
max seq, or after a TTL ≥ the max plausible reconnect window.

### 7.4 Recall-flush integration

On `Recall{root}` the holder:

1. **Flushes the contiguous WAL prefix** covering the recalled delegation via
   `Apply` and waits for the watermark ack.
2. Clears the overlay for that subtree and drops the delegation.
3. Sends `RecallAck{done}`.

The server's handoff barrier now means *"the holder's WAL for this region is
durably applied"* — so when the contender is unblocked it sees the holder's
deferred writes.

**Contiguous-prefix flush:** the WAL is one ordered seq-space, and the watermark
is a single monotone value, so a non-contiguous subset cannot be applied. Recall
flushes the contiguous prefix up to the last op touching the recalled delegation.
It may incidentally flush another live delegation's earlier ops (harmless —
independent, server-bound anyway); that delegation keeps its post-prefix ops
deferred.

**Recall-flush failure fail-closed:** if the flush fails, the client does not
send `RecallAck`. The server's recall **timeout** then expires without an ack,
and the server fails the handoff — the contender stays blocked or gets EAGAIN,
and the holder's subtree is poisoned EIO. This is the fail-closed path: correct
over fast. (See Known Gaps §7.7 for the current mechanism.)

### 7.5 Flush triggers

| Trigger | Behavior |
|---|---|
| `fsync`/`fdatasync` | Hard barrier — flush the prefix covering that file, synchronous, returns the real error (the truth-point). |
| Recall | Flush the contiguous prefix for the recalled delegation before `RecallAck` (§7.4). |
| Size cap | WAL bytes/ops over threshold → flush; backpressure — when full, the next deferred op blocks on a drain (bounds memory; degrades toward synchronous under slow WAN; never OOMs). |
| Interval | Periodic background flush — bounds the un-`fsync`'d loss window in wall-clock time. |
| Release / unmount | Flush all, then release. |

**Loss-window is a knob.** Interval (time) + size (bytes) bound the machine-death
loss window, configurable like the cache TTLs. `fsync`'d data is never in the
window.

### 7.6 Loud data-loss logging (§5.1)

Deferral makes data loss asynchronous and potentially silent; the **loud,
file-naming log is the required mitigation** that makes any loss auditable and
hand-recoverable. On any event that discards un-flushed WAL data, the client
emits an **ERROR-level** `log.Log` that **enumerates the affected file paths**
(not a count, the actual paths), plus the cause, the seq range, and the
`(identity, volume)` / delegation. A `WalDataLost` metric (events counter +
files-lost counter) accompanies the log so the loss is alertable.

Events that emit this log:

| Event | Logged detail |
|---|---|
| Permanent apply-failure (ordered halt) | The failed op's path + `fserr` + seq, and every still-deferred path after it (stuck behind the halt, discarded with the poisoned overlay) |
| Gen-fence discard on replay | Every fenced path + the revoked gen + seq range |
| Recall-flush failure | The recalled region's pending paths + `fserr` |
| WAL unreadable on startup (corrupt persist tier) | The WAL file + any entries recoverable enough to name |

**Honest limit — machine-death loss is server-side and region-level only.** When
the client machine dies, its WAL dies with it; no client-side enumeration is
possible. The server logs (ERROR) that a reaped/fenced session's region was
handed off with un-acked WAL, but it cannot name the lost files (it never
received them).

### 7.7 Known gaps / follow-ups

These are correct-by-construction in the shipped implementation but are not yet
optimized or fully signaled:

**(a) Resume-watermark optimization unwired.** The replay protocol is
specified to resume from `watermark+1` by fetching the server's durable watermark
on session resume. The `SetVolume` call is not wired in the mount path, so the
client sends an empty Volume on resume — the server returns a zero watermark and
the client replays from seq 0. The server-side dedup (`seq ≤ watermark` → skip)
makes this **correct and data-safe**; it just re-streams more ops than necessary.
Wiring the resume-watermark is a follow-up optimization.

**(b) Per-op caller fidelity for non-squash mapping modes.** WAL ops carry a
mount-level `Caller` (the principal that established the mount). This is correct
for squash mode (the default — all ops map to one identity). For `passthrough` and
`system` mapping modes, where the per-op caller matters for identity resolution,
the WAL caller may not reflect the original caller's identity. Per-op caller
fidelity for non-squash modes is a follow-up.

**(c) `RecallAck` has no explicit Abort/Error field.** When a recall-flush fails,
the client does not send a clean `RecallAck`; the server relies on the recall
**timeout** to fail the handoff (fail-closed). An explicit `RecallAck{error}` or
`RecallAbort` message would let the server fail faster and with a clearer signal.
This is a protocol follow-up.

---

## 8. Data-safety summary

| Scenario | Phase 1 | Phase 2 |
|---|---|---|
| `fsync`/`fdatasync` | Durable before return, always | Durable before return, always |
| Remote contention | Recall flushes → coherent, no loss | Recall flushes WAL prefix → coherent, no loss |
| Client process dies, machine lives | No deferred data; close already durable | Boot-epoch reclaim + gen-fence replays WAL from seq 0 via server-dedup → zero loss |
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
- **Multi-replica HA for the watermark store.** Designed-for via the
  `WatermarkStore` interface and `WithWatermarkStore` option seam; built in the
  cloud's Postgres impl, not the OSS default (bbolt).
