# WAN link saturation (SP4): writeback cache + readahead window

**Date:** 2026-05-26
**Status:** implemented; **the WRITE win (writeback) is strong and near-saturating
(+50%); the READ win (readahead window) does NOT hold up — it regresses in the
perf harness and is deferred to a partial-consume redesign.** The optimized perf
suite (`*Opt` benchmarks) tracks both configs in Bencher.

## Measured result — through the perf harness (VM, `GMOUNTIE_BENCH_TCP`, 50ms RTT / 100Mbit ≈ 11.9 MiB/s)

| WAN 64 MiB (bench) | baseline | Opt (writeback + window 16 × 256KiB) | verdict |
|---|---|---|---|
| **SeqWrite** | 6.84 MB/s | **10.28 MB/s (+50%, ~86% of ceiling)** | **win — ship** |
| **SeqRead** | 3.36 MB/s | **2.96 MB/s (−12%)** | **regresses — defer** |

**Write (writeback): a strong, near-saturating win — keep + recommend (opt-in).**

**Read (readahead window): does NOT robustly help; it regresses in the bench.**
The deep window's concurrent prefetches do not pipeline effectively on a
bandwidth-limited link, and the **one-shot-whole-chunk `Serve`** wastes any chunk
larger than a single FUSE read (so a deep window of large chunks fetches data
the consumer never serves from). An earlier *ad-hoc* fio run showed a fragile
+35% read at `chunk=128KiB` — but the organized bench (the authoritative
measurement) shows the window regressing. **Conclusion: the readahead window is
not a read win as implemented.** It needs the **partial-consume `Serve`
redesign** (large Read RPCs, serve sub-ranges, keep the chunk until drained) +
an investigation into why the deep window's prefetches don't overlap. Until then,
**leave `readahead_window` at its default 1** (the deep window can hurt); the
knob ships, but is not recommended for reads yet.

**Recommended WAN config (current):** `writeback_cache: true` (the real win);
`readahead_window: 1` (default — do NOT deepen until the partial-consume redesign).

**Writeback correctness — validated.** `test/e2e/fs` writeback suite passes on
real FUSE (VM): write-then-read-back (3 MiB > one max_write, exercises async
flush) and truncate-under-writeback both correct; the diagnostic's 64 MiB
round-trip across a remount confirms close-to-open. One limitation, **pre-existing
and NOT writeback-specific**: `node.go` `setattrAt` drops `FATTR_MTIME` (gMountie
has no `utimens`/mtime-mutation RPC at all), so explicit `utimes` and the kernel's
mtime-on-flush are no-ops — a separate future `Utimens` RPC, unchanged by this work.

**Deferred saturation lever (separate follow-up, not this spec):** rework the
readahead to large chunks (e.g. 1 MiB Read RPCs) with a **partial-consume**
`Serve` (serve sub-ranges, keep the chunk until drained) + deep window, and dig
into the write per-RPC overhead. That would push toward the ceiling without
streams-per-fd. Escalation to streams-per-fd (B) remains the last resort if
saturation is required and the partial-consume redesign falls short.

---

**Original status:** approved design, pending spec review → implementation plan
**Branch:** `worktree-sp4-bidi-open` (off `origin/master`)
**Scope:** client I/O only — `pkg/client/io` (readahead + prefetch driving),
`pkg/client/config` (a readahead-window knob), and validating the already-wired
opt-in FUSE `writeback_cache`. **No wire-protocol or fd-model change.**

## Context

This is **SP4** of the proto-v2 effort. The architect's original SP4 was
"bidi-Open: the stream *is* the fd." We rejected that shape after a reliability
analysis: on a WAN — the project's whole point — long-lived streams break
constantly (blips, server rolls, LB idle timeouts, GOAWAY), and "stream = fd"
makes every break an fd break, requiring NFSv4.1-session / SMB3-durable-handle
reconnect-and-replay machinery to match today's robustness. gMountie **already**
has the resilient pattern: the fd is durable session state (`RegisterFile`), the
session resumes on a broken stream (`grpc/session.go`), and every Read/Write is a
short-lived RPC wrapped in `retryableCall`. A transport blip retries one RPC; the
fd never dies.

So SP4 keeps that model and chases the throughput win a cheaper way. If this
proves insufficient (can't reach the link ceiling), the escalation is
**streams-per-fd (option B), with reconnect machinery budgeted as a first-class
part of that design** — not this one.

## Problem (measured — Bencher baseline on master, WAN profile = `delay 25ms 5ms rate 100Mbit`)

WAN sustained sequential I/O leaves the 100 Mbit (~11.9 MiB/s) pipe ~50–70% idle:

| WAN workload | now | ceiling | why |
|---|---|---|---|
| SeqWrite 64 MiB | ~6.5 MiB/s | ~11.9 | serial: kernel writes are synchronous (writeback off) |
| SeqWrite 1 MiB | ~2.2 MiB/s | RTT-dom. | (small-file is SP3's lane) |
| SeqRead 64 MiB | ~3.4 MiB/s | ~11.9 | serial: ~1 prefetch of 64 KiB in flight |

Two independent serializations, each ~RTT-bound:

- **Writes** are synchronous because the FUSE **writeback cache is off**
  (`DefaultFUSEWritebackCache = false`): the kernel waits for each WRITE before
  issuing the next, so `streamingWrite`'s blocking `CloseAndRecv` runs one chunk
  at a time. (The default comment says it's off "pending Phase 4's cache layer" —
  which now exists.)
- **Reads** prefetch **at most one 64 KiB chunk, one in flight** (`readahead.go`),
  so a sequential read stalls ~one RTT per 64 KiB.

## Goals

1. Saturate the WAN link on sustained sequential I/O — WAN SeqRead/SeqWrite from
   ~3.4–6.5 MiB/s toward the ~11.9 MiB/s bandwidth ceiling.
2. Preserve the durable-fd + retryable-RPC resilience — **no protocol/fd change.**
3. Writeback cache stays **opt-in** (`DefaultFUSEWritebackCache` remains `false`);
   SP4 validates and hardens it as a WAN-throughput mode, not the default.
4. Read-window depth is a **config knob**, defaulting conservatively.

## Non-goals

- **Streams-per-fd / bidi-Open** — the escalation path only, if A can't reach the
  ceiling. Not built here.
- **Making `writeback_cache` default-on** — it changes write semantics mount-wide;
  keep it opt-in until there's WAN evidence + bake-in.
- Any change to the wire protocol, the session/fd model, or `retryableCall`.

## Design

### Read half — N-deep readahead window (config-knobbed)

Today `Readahead` (`readahead.go`) holds a single prefetched chunk and arms one
prefetch one-chunk-ahead, "at most one outstanding prefetch at a time." Deepen it
to an **ordered window of up to `ReadaheadWindow` chunks**, with up to that many
Read RPCs in flight concurrently, kept filled ahead of the consumer:

- **New config:** `RpcConfig.ReadaheadWindow int` (number of `ReadaheadChunkBytes`
  chunks to keep prefetched/in-flight ahead). Validated `min=1,max=64`. Default
  `1` (preserves today's behavior exactly); WAN users raise it (e.g. 8–16) so
  `window × chunk` covers the bandwidth-delay product. `ReadaheadChunkBytes` and
  `ReadaheadThreshold` are unchanged.
- **`Readahead`:** replace the single `prefetched`/`prefetchedOff` slot with a
  window keyed by chunk offset (a small ordered ring of ≤ `ReadaheadWindow`
  entries, each "in-flight" or "ready"). `Observe` arms enough prefetches to fill
  the window ahead of `off+n`; `Serve` satisfies from whichever window entry
  covers `off`, consumes it, and slides the window forward (arming the next
  prefetch to keep the window full). Non-sequential `Observe` drops the whole
  window and re-arms (same heuristic as today, widened).
- **Prefetch driving (`backend_grpc.go`):** issue up to `ReadaheadWindow`
  concurrent Read RPCs (each the existing short-lived, `retryableCall`-able
  server-streamed Read) to fill the window. Bounded concurrency per fd.

This keeps the pipe full on sequential reads while every fetch stays an
independent, retryable RPC — resilience intact.

### Write half — validate + harden the opt-in writeback cache

`writeback_cache` is already wired (`common.go` toggles `CAP_WRITEBACK_CACHE`)
and config-exposed (`FUSEConfig.WritebackCache`, default off). With it **on**, the
kernel buffers writes in its page cache and flushes them asynchronously, issuing
up to `MaxBackground` (=64) concurrent WRITE ops — which the client's existing
per-op `streamingWrite` already handles in parallel → the link fills. SP4's work
is **not** the flag; it is making writeback-on correct and proven, since it was
deferred before the cache layer existed:

- **FUSE writeback contract:** under `CAP_WRITEBACK_CACHE` the kernel owns
  size/mtime for cached writes and expects the FS to honor kernel-supplied
  attributes on `setattr`/flush. Verify the node (`node.go` `Setattr`) and the
  server handle writeback-mode size/mtime correctly (no truncation/stale-size
  bugs).
- **Cache-layer interaction:** confirm the kernel writeback composes with the
  client `cachedBackend` (write-through-to-server + invalidate fires on the
  kernel's async flush, not the app's `write()`), and with the subscribe/validity
  layer (write visibility to other clients is delayed to flush — close-to-open
  consistency; the validity layer must still revalidate correctly).
- **Error reporting:** writeback moves write errors to `fsync`/`close` (FLUSH).
  SP3's `WriteAndFlush` already carries the close-tail flush; confirm a write
  error surfaces there as the `close()` errno.
- **Default stays off.** SP4 delivers writeback-on as a validated opt-in
  (`writeback_cache: true`) for WAN throughput, updates the now-stale "pending
  Phase 4" comment, and uses it for the perf measurement.

## Risks / consistency

- **Writeback relaxes write-visibility** to close-to-open (kernel buffers writes
  before the server sees them). Acceptable and standard for a network FS, but
  must be verified against the subscribe/validity invalidation timing — hence the
  opt-in default and the consistency tests below.
- **Readahead window memory & waste:** `ReadaheadWindow × ReadaheadChunkBytes`
  per actively-read fd; a seek discards the window (wasted prefetch). The `max=64`
  cap and the default of `1` bound the blast radius; the knob is opt-in-deep.
- **No resilience regression:** every prefetch/write is still a short-lived,
  retryable RPC against a durable session fd. (This is the property we are
  explicitly protecting.)

## Testing

- **Read window (unit, `pkg/client/io`):** window fills to `ReadaheadWindow` ahead;
  `Serve` hits across the window and slides; non-sequential `Observe` drops it;
  `ReadaheadWindow=1` reproduces today's single-slot behavior exactly;
  concurrent-prefetch bound respected.
- **Writeback correctness (e2e, `test/e2e`):** with `writeback_cache: true` —
  write-then-read-back returns the written bytes; truncate/size via `setattr`
  is correct; a write that fails server-side surfaces as a `close()`/`fsync`
  errno; with subscribe enabled, a second client sees the write after flush
  (close-to-open) and the validity layer revalidates.
- **Bencher before/after (acceptance):** dispatch `perf.yml --ref <sp4-branch>
  -f bencher_branch=sp4` with the client configured `writeback_cache: true` +
  a deep `readahead_window`, and compare WAN SeqRead/SeqWrite to the master
  baseline. **Target: climb toward ~11.9 MiB/s.** LAN benches unregressed.
- **LAN no-regression** with defaults (`writeback_cache: false`,
  `readahead_window: 1`) — behavior identical to master.

## Acceptance criteria

1. `ReadaheadWindow` knob exists (default 1 = today's behavior); the window
   deepens prefetch and keeps ≤ N Read RPCs in flight; unit-proven.
2. `writeback_cache: true` is correct: write-then-read-back, setattr/size, and
   `close()`-time error reporting all pass; composes with cache + subscribe.
3. **VM/Bencher: WAN SeqRead and SeqWrite climb materially toward ~11.9 MiB/s**
   with writeback-on + a deep readahead window; LAN unregressed.
4. Defaults unchanged (`writeback_cache: false`, `readahead_window: 1`) — no
   behavior change for current users.
5. `task lint` + `task test` pass.
6. If #3 cannot reach the ceiling, that is the documented trigger to escalate to
   streams-per-fd (option B) in a separate spec.

## Files expected to change

- `pkg/client/io/readahead.go` — single slot → N-deep window.
- `pkg/client/io/backend_grpc.go` — drive up to `ReadaheadWindow` concurrent
  prefetch Read RPCs; wire the new knob into the per-fd `Readahead`.
- `pkg/client/config/rpc.go` — `ReadaheadWindow` field + default + validation.
- `pkg/client/io/node.go` / server — writeback-mode `setattr`/size handling if the
  validation surfaces gaps.
- `pkg/client/config/fuse.go` / `mount/common.go` — update the stale "pending
  Phase 4" comment; no default change.
- Tests alongside each; e2e writeback-consistency tests.
