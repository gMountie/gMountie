# WAL — Write-Delegation Phase 2 design

**Status:** Design approved 2026-06-23. Ready for an implementation plan.
**Builds on:** Phase 1 (delegation + recall coherence layer), merged in PR #170.
**Supersedes:** the Phase 2 sketches in [docs/design/delegation-recall.md](../../design/delegation-recall.md) §7 and [docs/design/rwo-wal.md](../../design/rwo-wal.md) §4–7 — those recorded the *constraints*; this is the concrete design.

Phase 1 shipped the coherence layer (a client takes a write-delegation over a
subtree; the server arbitrates and recalls on contention) with `close()` still
flushing durably — no deferral, no data loss, no write-speed win. **Phase 2 adds
the WAL that turns the provable isolation into a write-speed win**: under a held
delegation, mutating ops defer into an on-disk log and flush in pipelined batches,
collapsing the per-file `open→write→close` RTT that dominates small-file WAN
workloads (`npm install`, build trees, `git checkout`).

Scope decision: Phase 2 is **one design + build** (not staged) — the per-fd
handle-layer seam refactor and the WAL ship together.

---

## 1. The overlay model — WAL + pending overlay (read-your-own-writes)

Two co-managed structures, kept consistent in one step on every deferred op:

- **WAL** — the on-disk, ordered, sequence-numbered op-log per `(identity,
  volume)`. The **single source of truth for un-flushed state.** Reuses the
  client cache's bbolt-backed persist machinery. Survives client-process restart.
- **Pending overlay** — an in-memory, `memfs`-shaped tree (path→pending node,
  **including tombstones** for deferred `unlink`/`rename`), rebuilt by scanning
  the WAL on startup.

Reads are served as **`server-acked base ⊕ pending overlay`**. The existing
two-tier cache accelerates the *base*; it never holds the only copy of pending
state (this kills the dual-write / eviction hole — un-flushed state is never only
in the evictable cache). Byte-range reads of a partially-flushed file overlay
pending bytes over base bytes.

**This is correct precisely because of the delegation.** The client is provably
alone under the subtree, so the local view (base ⊕ pending) is authoritative; no
remote writer can change it without a recall first.

`memfs` (`pkg/client/backend/memfs`) — a full in-memory `FileSystemBackend`
reference impl — was built explicitly so "the WAL / a write-batcher have a real
backend to decorate." Its node-tree (path→node, byte-slice files, tombstone-able)
is the natural overlay substrate; the overlay is its in-memory shape rebuilt from
the durable WAL.

---

## 2. Architecture — the handle-layer seam & WAL placement

### 2.1 The seam (prerequisite, built within this design)

Today per-fd write state (`WriteCoalescer`) lives on the transport's
`grpcFileHandle`, and `Flush`/`Release` drain it via the fused `WriteAndFlush`
RTT. Phase 2 lifts per-fd write state into a **composable per-fd handle layer** at
the reserved `posWritePath` slot (`client-architecture.md` §2.2/§9): the WAL
layer's handle wraps the transport's handle and participates in the `Unwrap()`
walk, so the transport still resolves the leaf fd. This is the delicate,
behavior-preserving refactor §9 flagged; it is perf-gated against Bencher (the
coalescer must not regress).

### 2.2 Coalescer ↔ WAL (two complementary tiers)

- **Coalescer** batches *bytes within one file*.
- **WAL** batches *ops across files*.
- **Delegated path:** the coalescer's drain target becomes "append a write-op to
  the WAL" instead of `WriteAndFlush`-to-wire; `close`/`create`/`rename`/`unlink`/
  metadata likewise append to the WAL (deferred) and update the overlay.
- **Un-delegated path:** byte-for-byte today's synchronous coalescer →
  `WriteAndFlush` → wire. The WAL activates *per-path on `IsDelegated`*; the
  un-delegated path is the natural fallback on recall/denial.

`posWritePath` hosts: the lifted coalescer, the WAL (append/flush/replay), and the
overlay (RYOW reads) — all gated by the Phase 1 `Manager`'s `IsDelegated`. The WAL
hangs off the `Manager` (which already owns grants + the recall stream).

The existing transport **"sticky write-back error"** (set when a coalesced flush
loses optimistically-acked bytes, surfaced on `Release`) is the async-error
pattern the WAL extends — not new machinery.

---

## 3. Sequence, generation & replay protocol (corruption-critical core)

Three durable concepts:

- **`seq`** — client-assigned, **monotone per `(identity, volume)`** (one seq
  space even across two disjoint delegations on the volume). Orders the WAL,
  drives dedup. Lives in the `WalOp` envelope (§3.2), not on unary requests.
- **`gen`** — the server stamps a generation on each granted delegation (Phase 1's
  table already stores `{session, identity, gen}`); the grant returns it. The
  client **tags every WAL op with the gen it was deferred under.** This is the
  fence.
- **Watermark** — server-durable per `(identity, volume)`: the highest seq durably
  applied. Replay of any op `seq ≤ watermark` is a no-op (dedup). Stored
  **alongside the revoked-gen set in one store** — if they diverge, fencing breaks.

### 3.1 Durable store: `WatermarkStore` interface, bbolt in OSS

`{watermark, revoked_gens}` per `(identity, volume)`, behind a `WatermarkStore`
interface, injected via the existing `server.New(cfg, ...Option)` extensibility
seam (`WithWatermarkStore`). **Default = embedded bbolt** (already a dependency via
the cache persist tier; keeps the OSS server a self-contained binary; file on the
server's local disk / the cloud's persistent volume → restart-safe). The **cloud
swaps a centralized (e.g. Postgres) impl** through the same Option — no fork,
wire-transparent, bounded surface. Single-node is sufficient today (OSS
single-server; cloud 1 replica); multi-replica HA is the cloud impl's concern, not
this design's.

### 3.2 Flush = a pipelined batch with one commit point

A flush is a **client-streaming RPC**: `RpcFs.Apply(stream WalOp) returns
(ApplyAck)`. `WalOp` is a `oneof` over the *existing* mutating request messages
(Create/Write/Mkdir/Rmdir/Rename/Unlink/Symlink/SetAttr/SetXAttr/RemoveXAttr)
**+ `seq` + `gen`** — no op-payload duplication; unary op messages are unchanged.
The client fire-hoses the batch (the app already got its local ack), half-closes;
the server:

1. applies each op **in seq order**, dispatching to the **same internal
   `applyOp`** the unary handlers use (refactor the unary handlers to call it —
   no duplication of identity-bind / emit / fs logic),
2. per op: `seq ≤ watermark` → skip (dedup); `gen` revoked → **reject** (fenced);
   else apply + advance the in-memory watermark,
3. at stream end: **persist the watermark to the store, then return
   `ApplyAck{watermark}`**.

That ordering is the **persist-before-ack invariant**: the client only drops WAL
entries `≤` the *acked* watermark, and the server only acks what it durably
recorded. **One RTT for the whole batch** — the WAN win — with no per-op durable
write.

### 3.3 Replay on reconnect (boot-epoch reclaim, PR #119)

Session resumes → server returns the client's current `watermark` for `(identity,
volume)` (new field on the resume reply) → client replays WAL ops with `seq >
watermark` in order via the same `Apply` stream → dedup + gen-fence apply
identically. Superseded ops (revoked gen — the machine-death-then-handoff case)
are rejected; the client discards that fenced WAL portion (the bounded-loss
scenario).

### 3.4 Error mid-batch (ordered halt)

Ordered WAL ⇒ **halt at first failure.** `ApplyAck` carries
`{committed_watermark, failed_seq, fserr}`. Permanent failures (ENOSPC / **EACCES**
/ EIO) → discard overlay + mark subtree EIO + release delegation (§5); ops after
the failure stay stuck and die with the poisoned overlay. Transient (EAGAIN) →
retry the batch from `committed_watermark + 1`.

> **EACCES is a real apply-failure case, not impossible.** A subtree delegation is
> ACL-checked at the *root* and covers *new* children — it does NOT grant write
> access to arbitrary *existing* files under it. Permission failures go down the
> same async path as ENOSPC/EIO.

### 3.5 Generation lifecycle + GC

Grant stamps `gen=G` (returned). On recall/handoff the server **durably records
`G` revoked in the store before serving the contender** (extends Phase 1's handoff
barrier). Revoked-gens are bounded: a `G` may be GC'd once no WAL could still
replay it — once `(identity, volume)`'s watermark passes `G`'s max seq, or after a
TTL ≥ the max plausible reconnect window.

---

## 4. Recall-flush integration & flush triggers

### 4.1 Recall now flushes before handoff

On `Recall{root}` the holder: (1) **flushes the WAL prefix** covering the recalled
delegation via `Apply` and waits for the watermark ack; (2) clears the overlay for
that subtree + drops the delegation; (3) sends `RecallAck{done}`. The server's
handoff barrier (Phase 1 §4) now means *"the holder's WAL for this region is
durably applied"* — so when the contender is unblocked it sees the holder's
deferred writes. This is what makes the **Phase 2 read-recall rule** real: a
reader/`readdir` under the subtree forces the holder's deferred creates durable
before the read proceeds.

### 4.2 Contiguous-prefix flush

The WAL is one ordered seq-space, but cross-subtree ops are forced *synchronous*
(Phase 1), so different delegations' ops are independent — yet the watermark is a
single monotone value, so a non-contiguous subset cannot be applied. **Recall
flushes the contiguous prefix up to the last op touching the recalled
delegation.** It may incidentally flush another live delegation's earlier ops
(harmless — independent, server-bound anyway); that delegation keeps its
post-prefix ops deferred. The watermark stays a simple high-water mark.

### 4.3 Recall-flush failure aborts the handoff

If the server cannot durably apply the holder's flushed WAL (ENOSPC/EIO), it does
**not** serve the contender — the contender stays blocked / gets EAGAIN, the
holder's subtree is poisoned EIO, and the delegation is not cleanly released. The
region stays stuck until the holder reconnects or is reaped. Correct over fast.

### 4.4 Flush triggers

| Trigger | Behavior |
|---|---|
| `fsync`/`fdatasync` | Hard barrier — flush the prefix covering that file, **synchronous**, returns the real error (the truth-point). |
| Recall | Flush the contiguous prefix for the recalled delegation before `RecallAck` (§4.1–4.2). |
| Size cap | WAL bytes/ops over threshold → flush; **backpressure** — when full, the next deferred op blocks on a drain (bounds memory; degrades toward synchronous under slow WAN; never OOMs). |
| Interval | Periodic background flush — bounds the un-`fsync`'d loss window in wall-clock time. |
| Release / unmount | Flush all, then release. |

**Loss-window is a knob.** Interval (time) + size (bytes) bound the machine-death
loss window, configurable like the cache TTLs, conservative defaults (≈ a few
seconds / a few MB). `fsync`'d data is never in the window.

---

## 5. Error & durability model (the write-back contract)

Deferral changes **when** write errors surface, not **whether** — the standard
write-back trade:

- **The truth-point is `fsync`, not `close`.** Plain `close()` does not reliably
  surface writeback errors (fsyncgate; POSIX-legal). FUSE `Flush`-on-close *can*
  carry an error back to `close()` today; deferring trades that close-time error
  fidelity for write speed — exactly what buffered I/O does.
- **RYOW + async-failure coupling (the crux).** Because reads are served from the
  overlay, a *late* apply-failure means the client already served reads of data
  that turned out non-durable. So on a permanent apply-failure the overlay is
  **discarded/invalidated, the subtree marked EIO + stale, and the delegation
  released**; inodes the app still holds are now divergent (EIO until reopen).
- **Data-loss window** = machine death **or** a partition longer than the grace
  period (the server cannot distinguish dead from partitioned). Un-`fsync`'d WAL
  data in that window is lost — identical to a local FS losing dirty page cache on
  power loss, and to NFS/SMB lease expiry. Target workloads (build trees,
  `node_modules`, scratch, checkpoints) regenerate it.

Data-safety summary (extends delegation-recall.md §8):

| Scenario | Phase 2 |
|---|---|
| `fsync`/`fdatasync` | Durable before return, always |
| Remote contention | Recall flushes the WAL prefix → coherent, no loss |
| Client process dies, machine lives | Boot-epoch reclaim + gen-fence replays WAL from watermark → zero loss |
| Machine death or partition > grace | Bounded dirty-WAL window lost — same as local-FS power loss |

---

## 6. Components & file structure

**Server (`pkg/server/`):**
- `watermark/` (new) — `WatermarkStore` interface + bbolt impl; `{watermark,
  revoked_gens}` per `(identity, volume)`. Injected via `server.New(...Option)`
  (`WithWatermarkStore`; default bbolt).
- `controller/` — new `Apply(stream WalOp) returns (ApplyAck)` handler; refactor
  the mutating unary handlers to call a shared internal `applyOp(ctx, identity,
  op)`. Dedup + gen-fence + persist-before-ack; ordered-halt error reporting.
- `delegation/` (extend) — on handoff, durably record the revoked gen via the
  store *before* serving the contender.
- Session resume (PR #119 path) — return the current watermark.

**Wire (`api/proto/`):** `Apply` RPC + `WalOp` (`oneof` over existing mutating
request messages **+ `seq` + `gen`**) + `ApplyAck{watermark, failed_seq, fserr}`;
resume reply gains `watermark`. Unary op messages unchanged.

**Client (`pkg/client/backend/`):**
- `wal/` (new) — durable op-log (bbolt/persist tier) + seq assignment + the
  `memfs`-shaped pending overlay (tombstones, byte-range) + flush/replay. Hangs
  off the Phase 1 `Manager`.
- Handle-layer seam — lift `WriteCoalescer` off the transport `grpcFileHandle`
  into a composable per-fd handle at `posWritePath` (Unwrap chain preserved).
- `delegation/layer.go` (extend) — route mutating ops → WAL+overlay when
  `IsDelegated`, else today's pass-through; reads consult `base ⊕ overlay` for
  delegated paths; recall handler gains flush-before-ack; reconnect replays via
  `Apply`.

---

## 7. Testing

- **Unit:** WAL ordering/seq; overlay `base ⊕ pending` (tombstones, byte-range);
  watermark dedup; gen-fence; bbolt `WatermarkStore` durability-across-restart;
  backpressure.
- **Reference-backend:** decorate `memfs` with the WAL layer (built for this) —
  WAL semantics with no server/FUSE.
- **Contract:** `RunBackendContract` must still pass with the WAL in the chain
  (close-to-open **+ read-your-own-writes**).
- **Corruption/crash (critical):** kill mid-flush → replay dedups (no
  double-apply); superseded replay (revoked gen) → fenced; ordered-halt on
  apply-error → subtree poisoned; WAL survives client-process restart (boot-epoch
  reclaim).
- **E2E (bufconn + netem):** the headline — an `npm install`-like create-heavy
  workload over simulated WAN showing the per-file-RTT collapse (the actual win);
  two-client contention → recall-flush → coherent; machine-death sim → bounded
  loss + replay.
- **Perf (Bencher):** the WAN small-file-write benchmark — where Phase 2's value
  is *proven* (Phase 1 was a no-op on writes). This is the win/regression gate.

---

## 8. Highest-risk items (for the plan)

1. **The seam refactor** — delicate, behavior-preserving; perf-gate the coalescer
   didn't regress.
2. **Persist-before-ack** — corruption if violated.
3. **Crash/replay dedup + gen-fence durability + GC** — the corruption-critical
   core; the fence must be durable in the *same* store as the watermark.
4. **RYOW across eviction** — the overlay (not the cache) is the source of truth
   for un-flushed state.
5. **Recall-flush failure aborting the handoff** — the server must never serve a
   contender on an unconfirmed flush.

---

## 9. Out of scope (unchanged from Phase 1)

Volume-level exclusion (RWO); per-file delegation; read-only-many (ROX) leases;
mount-time capability negotiation (deferral is demand-driven per delegation, no
mount mode). Multi-replica HA for the watermark store is the cloud `WatermarkStore`
impl's concern, designed-for via the interface but not built here.
