# RWO Lease + On-Disk WAL

**Status:** Paused — design recorded for when the feature resumes.
**Last updated:** 2026-06-22

The RWO lease and WAL feature is on the roadmap but not currently being
implemented. This document records the durable design decisions so the work can
resume from a solid footing without re-litigating settled questions.

The extensibility refactor shipped on this branch ([client-architecture.md](client-architecture.md))
deliberately accommodates the WAL's slots in the layer stack; this doc records
the motivation, the lease model, the failure model, and the one decided fork.

---

## 1. Problem

Small-file workloads over WAN are round-trip-bound, not bandwidth-bound:
`npm install`, `git checkout`, build trees — thousands of `open → write → close`
sequences, each paying RTTs that serialize at 60–150 ms.

Per-fd `WriteCoalescer` + `WriteAndFlush` already batch bytes *within* one file
and ack optimistically before durability. But coalescing is per-fd, and the
cross-file `Compound` RPC was removed in the protocol revamp (PR #103–#107). A
10 000-tiny-file storm still pays O(files × RTT): every file its own open /
flush / release round-trips.

To go sub-linear in RTT for that workload, writes must be batched *across files*
and each `close()` must stop blocking on a round-trip. That deferral is only
safe when there is no concurrent reader/writer to be incoherent with — which is
the whole design.

---

## 2. Access-mode lease model

A **volume-level reader/writer lease** is negotiated at mount time, using
Kubernetes access-mode vocabulary:

- **RWX** (shared) — multiple RWX mounts coexist; close-to-open semantics,
  unchanged from today. Conservative default.
- **RWO** (exclusive) — excludes every other mount of the volume. Because the
  writer is provably alone, the client may batch aggressively and defer closes
  through an on-disk WAL.

Lease admission lattice (server-enforced; first mounter sets the regime):

| Held \ Requested | RWX        | RWO        |
|------------------|------------|------------|
| (none)           | grant      | grant      |
| RWX (shared)     | grant      | **reject** |
| RWO (exclusive)  | **reject** | **reject** |

Single-writer is **guaranteed by the server**, never advisory — that guarantee
is what makes deferral safe. A rejected mount either fails fast or waits for the
holder to release or be reaped (implementation choice left for when the feature
resumes).

This aligns with the cloud volume model: ZFS-LocalPV volumes are node-local
RWO by nature, so the fast path is the common case there.

---

## 3. Phase 1 — Lease layer (no WAL)

Shippable independently. Delivers a correctness feature (guaranteed single
writer), not yet a speed one.

- Mount carries a requested access mode; server admits per the lattice and
  records the lease against the session.
- Lease lifetime rides the existing session machinery: a dead holder's
  exclusive lease is released by the grace-period reap
  (`server.session.grace_period`). No new liveness subsystem needed.
- RWO clients behave exactly as today otherwise. This proves out exclusivity
  and lease handoff with no data at risk before Phase 2 puts data behind it.

**Lease state ownership.** Keep it off the closed `sessionImpl` struct; a side
`LeaseManager` on `AppContext`, keyed by volume, holders indexed by session.
Acquire at `WhoAmI` (the one volume-scoped per-mount RPC); enforce on every
mutating RPC (not just at acquire — a client can skip `WhoAmI` and write
directly, so acquire-time admission alone is not hard enforcement); release on
every session-death path (`reap`/`ReleaseAll`/`ReapIf`/`Stop`/grace-expiry);
reclaim own lease on `Resume`. Serialize the lease check after ACL/bind, before
the io call.

---

## 4. Phase 2 — On-disk WAL + replay

Built on Phase 1's lease. The throughput payoff.

In RWO mode, deferred operations (closes, creates, metadata bursts, coalesced
writes) append to an **ordered, sequence-numbered log on the client's local
disk**. The server applies them idempotently; since ops already carry a
`request_id`, the WAL is a durable, ordered queue of idempotent requests and
replay is "re-send everything after the last server-acked sequence number."

**Flush triggers:** interval, accumulated size, explicit `fsync`, and
lease-release. `fsync` / `close`-with-`fsync` are **hard durability barriers**
— anything the application explicitly durabilizes is forced through immediately.

**Durability contract by mode:**

| Mode | Coherence | `close()` | `fsync()` | Crash before flush |
|------|-----------|-----------|-----------|-------------------|
| RWX  | close-to-open (today) | per today | durable | per today |
| RWO  | none — exclusive | acked locally, batched | hard flush barrier | replays if machine alive; bounded window lost only if machine dies |

The lost-on-machine-death window is an accepted risk for RWO's target workloads
(scratch, checkpoints, build trees). `fsync`'d data is always safe.

---

## 5. Failure model

Two independent sub-problems when the session dies with an unflushed WAL:

**1. Stuck lease.** Grace-period reap releases the exclusive lease when the dead
session is reaped. (Open question: does a waiting mounter block for the grace
window or fail fast? Decide when the feature resumes.)

**2. Unflushed WAL.** The **boot-epoch-gated session reclaim** (PR #119, see
[reliability-and-recovery.md](reliability-and-recovery.md)) acts as referee for
the lease handoff:

- **Same client reconnects within grace (boot epoch matches)** → replay WAL
  from last-acked seq. This is the pod-restart / network-blip case.
- **Grace expires / new epoch / different client** → declare old WAL dead,
  discard *before* handing off the lease.
- **Client machine/disk gone** → the unflushed window is lost, irreducibly —
  equivalent to a local FS losing dirty page cache on power loss.

> **CAVEAT — boot-epoch gating ≠ replay dedup.**
> Boot-epoch gating makes the lease *handoff* safe; it does nothing for
> *deduplicating* replayed operations. They are two separate mechanisms.
> The server's current idempotency cache is per-session, LRU-by-count (4096),
> no TTL, and discarded on session reap. A client reconnecting after a crash
> gets a new session with an empty cache; its replayed ops carry their original
> UUIDs — so any op the server applied-but-didn't-ack before the drop will be
> **re-applied**. "Zero data loss on reconnect" is an unearned claim until
> replay dedup is implemented (see §6, decided fork).

**Load-bearing handoff invariant.** Never hand the exclusive lease to a new
writer while a recoverable WAL might still exist. The boot-epoch gate is the
arbiter for the lease handoff: same epoch within grace → resume + replay + keep
lease; anything else → discard WAL, then hand off. This ordering rule is the
core handoff correctness argument — necessary but not sufficient for replay
correctness (which additionally requires cross-session dedup, §6).

---

## 6. Decided fork — replay dedup

The existing `request_id` idempotency cache is a retry-window dedup, not a
replay-horizon dedup: per-session, LRU-by-count, no TTL, dropped on reap,
keyed on a random UUID with no ordering. It is wrong on both axes for WAL
replay (not durable across sessions; not keyed on anything stable). There is
also no sequence primitive anywhere today — a client-assigned monotone seq per
`(identity, volume)` is net-new end to end.

The two viable designs were:

**(a) Durable server-side seq-watermark** per `(writer-identity, volume)`:
replay of any op `≤ watermark` is a no-op; survives restart; always safe.
Most robust, but adds durable server state and a new wire seq on every
mutating request.

**(b) Client-side replay from seq cursor + per-op idempotency audit.**
Absolute-offset writes are already idempotent; but `O_EXCL`-create, `rename`,
and other state-dependent ops are not — they must carry a generation, be
excluded from deferral, or force a synchronous flush. An explicit per-op audit,
not "accept corruption."

**Decision (2026-06-22): fork (a).** The server holds a durable
per-`(identity, volume)` seq-watermark. Replay of any op `≤ watermark` is a
no-op. Prefer an SQL store for the watermark **if** its per-op latency
benchmarks acceptably on the hot path — measure before committing (a
write-path round-trip to SQL per op could dominate the latency budget). The
implementation is deferred with the rest of Phase 2.

---

## 7. Remaining open questions (for when the feature resumes)

- **Mount-time mode negotiation surface.** Where does the requested access mode
  live — a mount flag, a field on an existing session/mount RPC, or a new lease
  RPC? This may force a proto change and touch `SingleVolumeMounter`.
- **Reject-vs-wait** behavior for an incompatible mount and the grace-window
  interaction.
- **WAL boundary in the layer stack.** Does the WAL sit below the existing
  per-fd coalescer (becoming the persistence tier the coalescer drains into),
  or is it a new backend layer? The per-fd handle-layer seam (see
  [client-architecture.md](client-architecture.md) §9) must be built first.
- **SQL store latency benchmark.** Run before committing to SQL for the
  seq-watermark (fork (a)).
- **ROX (read-only-many)** is a later freebie — read-only mounts can cache with
  long TTLs since nothing mutates. Out of scope for Phase 1/2.
