# Access-mode leases + on-disk WAL — abstract design

**Status:** draft / abstract. Deliberately high-level — names, signatures, and
proto shapes are *not* fixed here. Expect a refactor pass before we write the
implementation plan.

**Date:** 2026-06-22
**Branch:** `worktree-rwo-wal`

---

## Problem

Small-file workloads over WAN are **round-trip-bound**, not bandwidth-bound.
`npm install`, `git checkout`, build trees: thousands of
`open → write → close` (+ `lookup`/`setattr`) sequences, each costing RTTs that
serialize at 60–150 ms.

What we already do helps the *interior of one file* but not the
*many-files* case:

- Per-fd `WriteCoalescer` + `WriteAndFlush` already batch the bytes within a
  single open file and ack optimistically before durability.
- But coalescing is **per file descriptor**, and the cross-file `Compound` RPC
  was **removed** in the protocol revamp. So a 10 000-tiny-file storm still pays
  ~O(files × RTT): every file its own open/flush/release round-trips.

To go sub-linear in RTT for that workload we must batch *across files* and
**stop blocking each `close()` on a round-trip** — i.e. defer acknowledged
closes. Deferral is only safe when there is no concurrent reader/writer to be
incoherent with. That observation is the whole design.

## Core idea

Cut on a **volume-level reader/writer lease, negotiated at mount time** (not
declared in static config), borrowing K8s access-mode vocabulary:

- **RWX** = *shared* lease. Multiple RWX mounts coexist. Today's close-to-open
  semantics, unchanged. Conservative default.
- **RWO** = *exclusive* lease. Excludes every other mount of the volume. Because
  the writer is provably alone, the client may batch aggressively and defer
  closes through an on-disk WAL.

Lease admission lattice (server-enforced, first mounter sets the regime):

| held \ requested | RWX        | RWO        |
|------------------|------------|------------|
| (none)           | grant      | grant      |
| RWX (shared)     | grant      | **reject** |
| RWO (exclusive)  | **reject** | **reject** |

A rejected mount either fails fast or waits for the holder to release / be
reaped (knob, see open questions). Single-writer is **guaranteed by the server**,
never advisory — that guarantee is what makes deferral safe rather than a
silent-corruption footgun.

This aligns with where we already are: cloud volumes are ZFS-LocalPV, which is
node-local **RWO by nature**, so the fast path is the common case there.

## Phase 1 — the lease layer (no WAL)

Shippable on its own; delivers a *correctness* feature (guaranteed single
writer), not yet a speed one.

- Mount carries a requested access mode; server admits per the lattice above and
  records the lease against the session.
- Lease lifetime rides the **existing session machinery**: a dead holder's
  exclusive lease is released by the **grace-period reap**
  (`server.session.grace_period`). No new liveness subsystem.
- RWO clients behave exactly as today otherwise — they just hold an exclusive
  lock. This lets us prove out **exclusivity + lease handoff** with *no data at
  risk* before Phase 2 puts data behind it.

## Phase 2 — on-disk WAL + replay

Built on Phase 1's lease. The throughput payoff.

- In RWO, deferred operations (closes, creates, metadata bursts, coalesced
  writes) append to an **ordered, sequence-numbered log on the client's local
  disk**. The server applies them idempotently; since we already dedup by
  `request_id`, the WAL is essentially a durable, ordered queue of idempotent
  requests, and replay is "re-send everything after the last server-acked
  sequence number."
- **Flush triggers:** interval, accumulated size, explicit `fsync`, and
  lease-release. `fsync`/`close`-with-`fsync` remain a **hard durability
  barrier** — anything the app explicitly durabilizes is forced through.
- The log lives **on local disk** so it survives process/session death and can
  be replayed on reconnect (the deliberate complexity we accepted).

### Failure model — "what if the session dies"

Two independent sub-problems:

1. **Stuck lease.** Grace-period reap releases the exclusive lease when the dead
   session is reaped. (Open: does a waiting mounter block for the grace window
   or fail fast?)
2. **Unflushed WAL.** Resolved by the **boot-epoch-gated session reclaim
   (#119)** acting as referee:
   - **Same client reconnects within grace (boot epoch matches)** → replay WAL
     from last-acked seq → **zero data loss**. This is the pod-restart /
     network-blip case — the common one, and #119's whole reason to exist.
   - **Grace expires / new epoch / different client** → old WAL is declared dead
     and discarded *before* the lease is handed off.
   - **Client machine/disk gone** → the unflushed window is lost, irreducibly —
     identical to a local FS losing dirty page cache on power loss, and an
     accepted risk for RWO's target workloads (scratch, checkpoints, build
     trees). `fsync`'d data is safe.

### Load-bearing invariant

**Never hand the exclusive lease to a new writer while a recoverable WAL might
still exist.** The boot-epoch gate is the arbiter: same epoch within grace →
resume + replay + keep the lease; anything else → discard WAL, *then* hand off.
Two clients' logs must never race over the same volume. This single ordering
rule is the core correctness argument.

## Durability contract (summary)

| Mode | Coherence with others | `close()` | `fsync()` | Crash before flush |
|------|----------------------|-----------|-----------|--------------------|
| RWX  | close-to-open (today) | per today | durable   | per today |
| RWO  | none — exclusive      | acked locally, batched | **hard flush barrier** | replays if machine alive; bounded window lost only if machine dies |

## Out of scope (for now)

- **ROX** (read-only-many): a later freebie — read-only mounts can cache with
  long TTLs since nothing mutates.
- Conflict/merge resolution — *by construction* there are no concurrent writers
  to conflict, so none is needed.
- Auto-detecting "spammed with small ops" and self-switching modes —
  **explicitly rejected**: the access mode is a declared, negotiated property,
  not a heuristic the client flips into under load. Determinism over cleverness.

## Open questions / likely refactor points

These are the things to settle in the "let's talk" pass before the plan:

1. **Mount-time mode negotiation surface.** Where does the requested mode live —
   a mount flag, a field on an existing session/mount RPC, or a new lease RPC?
   May force a proto change and touch `SingleVolumeMounter`.
2. **Reject-vs-wait** behavior for an incompatible mount, and the
   grace-window interaction.
3. **WAL boundary.** Does the WAL sit *below* the existing per-fd coalescer (it
   becomes the persistence tier the coalescer drains into), or is it a new layer
   in `pkg/client/cache`? This is the biggest structural question and the most
   likely refactor — today's write path (write-through cache → coalescer →
   `WriteAndFlush`) probably needs reshaping so deferred closes and cross-file
   ordering have a natural home.
4. **Server-side replay/apply path** and how it reuses (or outgrows) the
   `request_id` idempotency cache — that cache's retention has to cover the WAL
   replay horizon, or replay double-applies.
5. **Lease state ownership** — does it belong to `SessionManager`,
   `VolumeService`, or a new component, and how does it serialize with
   bind-identity/ACL checks already done per request.
