# SP5 — Partial-consume, pipelined readahead — Design

**Date:** 2026-05-27
**Status:** approved design, pending spec review → implementation plan
**Branch:** `worktree-sp5-readahead` (off `origin/master`)
**Scope:** client only — `pkg/client/io/readahead.go` (the readahead engine),
`pkg/client/io/backend_grpc.go` (the Read/prefetch drive, unchanged shape),
`pkg/client/config/rpc.go` (default tuning). **No wire-protocol, server,
fd-model, or cache-layer change.**

## Goal

Make client-side readahead actually effective for sequential reads on
high-RTT links: keep several large Read RPCs in flight so the WAN read pipe
stays full, instead of being round-trip-bound at one Read at a time. This is
the deferred read-path win from the WAN-link-saturation work — the lever that
lets sequential read throughput climb toward the link ceiling.

## Context — why readahead is a no-op today

The `read-path-allocs` work (already merged) deliberately made readahead a
**clean no-op** for real readers, to stop a wasted-prefetch allocation
regression. Two guards in `pkg/client/io/readahead.go` do this:

1. `Observe(off, n)` returns `nil` (arms nothing) when `n > chunkSize`
   (`readahead.go` ~line 105). A single 64 KiB chunk can never satisfy a
   larger read, so prefetching one was pure waste.
2. `Serve(dest, off)` is **whole-chunk-or-miss with one-shot consume**: it
   hits only if a single ready chunk fully covers `dest` (`len(dest) > end-off`
   → miss), and on a hit it **discards the entire chunk** even if the read
   consumed only part of it (`readahead.go` ~lines 155, 159).

The net effect: any reader whose request size differs from the chunk size — the
common case, including the perf bench's 256 KiB reads against the 64 KiB chunk
default, and the cache layer's 1 MiB-aligned misses — gets **zero** readahead.
Sequential reads fall to the synchronous path: one Read RPC, wait a full RTT,
next Read RPC. On a 50 ms link this leaves the pipe mostly idle.

The WAN-link-saturation experiment found that simply deepening the window
(`readahead_window > 1`) did **not** help and even regressed. The cause was not
a fundamental inability to overlap — `doPrefetch` issues each prefetch as its
own goroutine and its own streaming Read RPC, which multiplex fine over the
shared HTTP/2 connection — it was the chunk-size/`Serve` mismatch above: the
prefetched 64 KiB chunks could never be served, so the deep window just burned
RPCs. Fix the servability and the deep window pipelines as intended.

## Non-goals

- **No wire-protocol or server change.** `ReadStreamer.Stream` (server) already
  streams arbitrary sizes via a pooled frame buffer (landed in
  `read-path-allocs`); a "large Read RPC" is just a bigger `Size` on the
  existing `ReadRequest`.
- **No move of readahead into the cache layer.** Readahead stays in
  `BackendClient`, below the cache decorator. The two are orthogonal: the cache
  is the durable chunked store; readahead is the in-flight pipelining layer. When
  the cache is enabled, a `Serve` hit returns up through `cachedBackend.Read`,
  which stores it in `chunks/` naturally — no duplication. (Verified: the cache
  decorator runs no prefetch loop of its own; its only goroutine is the Subscribe
  consumer.)
- **No prefetch-buffer pooling** in this effort (see Memory footprint).
- **No per-fd bidirectional streams** (the SP4-C escalation, rejected earlier on
  fd-resilience grounds).
- **No partial/short FUSE reads.** `Serve` is full-or-miss; FUSE reads stay
  full-size.

## Design

Approach: **evolve the existing chunk-slot model** rather than replace it with a
sliding ready-buffer. Smaller blast radius on already-tested code, and the
chunk-keyed structure suits the cache-on case where reads are 1 MiB
chunk-aligned (one chunk ≈ one read; the deep window does the work).

### `Readahead` state (unchanged shape, one addition)

The `raChunk` slot today is `{off int64; data []byte}`. Add a per-chunk consumed
cursor so partially-consumed chunks survive:

```go
type raChunk struct {
	off      int64  // file offset where this fetch was issued
	data     []byte // nil while in flight; bytes once Stored
	consumed int    // bytes already served from the front of data (partial consume)
}
```

The live (still-servable) byte range of a ready chunk is
`[off+consumed, off+len(data))`.

### `Serve(dest, off)` — partial-consume, cross-chunk, full-or-miss

Replace the whole-chunk-or-miss body with a copy that walks contiguous ready
chunks:

1. Find the ready chunk whose live range contains `off` (`c.off+c.consumed <=
   off < c.off+len(c.data)`). If none, **miss** `(0, false)`.
2. Copy forward into `dest`, advancing across chunks: each chunk contributes
   `c.data[off-c.off : ...]` up to the lesser of (chunk end, remaining `dest`).
   To continue past a chunk boundary the **next** chunk must be ready and
   **contiguous** (`next.off == prev.off + len(prev.data)`); if not, **miss**.
3. **Hit** only when the full `len(dest)` is covered. On a hit, mark consumed:
   for each chunk the read advanced through, bump `consumed`; **drop any chunk
   that is now fully drained** (`consumed == len(data)`). A chunk the read only
   partially consumed keeps its remaining tail and stays in the window.
4. On a miss, copy nothing and mutate nothing (the synchronous Read path in
   `backend_grpc.go` fetches the whole `dest`). Stale eviction is `Observe`'s
   job, not `Serve`'s — unchanged.

Because step 3 only commits mutations once the full range is known covered, a
miss is side-effect-free and a hit is exact.

### `Observe(off, n)` — arm large chunks, deep window, any read size

- **Remove the `n > chunkSize` no-op guard.** Big reads are now servable.
- Eviction rule (the seek/gap path), restated for partial consume: **drop any
  chunk whose live range is entirely at/behind the new cursor** — i.e.
  `c.off + int64(len(c.data)) <= next` for ready chunks (`next = off+n`). For an
  in-flight chunk (`data == nil`) keep the existing `c.off + chunkSize <= next`
  test. A chunk the cursor sits inside is retained.
- After `threshold` strictly-sequential observations, arm up to `window` chunks
  of `chunkSize`, contiguous, starting at the first offset not already queued —
  same arming loop as today, just no longer gated by read size. Return the new
  offsets for the caller to prefetch.
- **Keep `threshold` (default 3).** A false-positive prefetch now costs
  `window × chunkSize` of WAN traffic instead of one 64 KiB chunk, so the
  sequential-run gate matters more, not less. Default unchanged.

### Drive — `backend_grpc.go` (no structural change)

`Read` keeps its current shape: try `Serve`; on hit, `Observe` + `go
doPrefetch` for each returned offset, return; on miss, synchronous streaming
Read, then `Observe` + prefetch. `doPrefetch` keeps issuing one streaming Read
per chunk under `h.lifeCtx`; several run concurrently and multiplex over HTTP/2.
The only behavioural change flows from the engine: with servable chunks and a
deeper window, `Serve` now hits in steady state and the prefetches overlap.

### Configuration / tuning defaults (`pkg/client/config/rpc.go`)

| Key | Today | SP5 default | Rationale |
| --- | --- | --- | --- |
| `readahead_chunk_bytes` | 64 KiB | **negotiated frame size (1 MiB)** | One prefetch = one server frame; the server frame pool is sized for it. Capped by the `Version` handshake exactly like `MaxWrite`. |
| `readahead_window` | 1 | **4** | A BDP-derived start for ~50 ms / 100 Mbit. Knob range stays `[1,64]`; operators on fatter/longer pipes raise it. |
| `readahead_threshold` | 3 | 3 (unchanged) | Sequential-run gate; still the right signal. |

The chunk-size default should track the negotiated frame size rather than a
hard-coded 1 MiB, so it stays correct if the server advertises a smaller frame.
Where the negotiated value is already available to the client at readahead
construction (the same value used to cap `MaxWrite`), use it; otherwise default
to `DefaultFrameSizeBytes`. The implementation plan pins the exact wiring.

### Memory footprint (explicit decision: acknowledge and defer)

Retaining chunks until fully drained means up to `window × chunkSize` bytes held
per open fd, unpooled — at the new defaults, ~4 × 1 MiB = **~4 MiB per open fd**,
freed when the chunk drains or is evicted. `read-path-allocs` deferred pooling
`doPrefetch`'s buffer precisely because it is retained until served/evicted;
SP5 turns that latent cost into a real (modest) one.

**Decision: acknowledge and defer.** Ship SP5 without pooling. If Bencher's
read-path `B/op` / `_substrate` series flags the allocation cost, add a
follow-up: a `sync.Pool` of `chunkSize` slices on `Readahead`, taken in `Store`
and returned at drain/eviction. That return path (through both `Serve`-on-drain
and `Observe`-on-eviction) is the non-trivial part and is not worth building
speculatively.

## Data flow (steady state, sequential read, cache off)

```
FUSE Read(dest, off)
  → BackendClient.Read
     → Readahead.Serve(dest, off)
        hit: dest covered by ready chunk(s); copy; advance consumed; drop drained
     → Readahead.Observe(off, n) -> [off+k·chunk, ...]   (refill the window)
     → for each: go doPrefetch  (streaming Read RPC, multiplexed over HTTP/2)
     → return n
  ... next sequential Read drains the retained tail / next ready chunk ...
```

At startup (cold window) the first few reads miss → synchronous Read fills them,
`Observe` arms the window, and prefetch goroutines race ahead; within a few
reads the window is full and `Serve` hits dominate.

## Error handling

- `doPrefetch` errors are swallowed (as today): a failed prefetch simply leaves
  that slot in-flight/absent; the next `Serve` misses and the synchronous Read
  refetches. No correctness impact.
- A prefetch returning short (server EOF before `chunkSize`, i.e. near
  end-of-file) stores a `data` shorter than `chunkSize`; `Serve`'s
  live-range/contiguity math already uses `len(data)`, so a short tail chunk is
  served correctly and the window naturally stops past EOF.
- Cancellation: prefetch goroutines run under `h.lifeCtx`; `Release` cancels
  them — unchanged.

## Testing

### Unit (`pkg/client/io`, testify suite on `Readahead`) — deterministic, no FUSE
- **Partial-consume retention:** `Store` a 1 MiB chunk at off 0; `Serve` 256 KiB
  at 0 → hit, returns 256 KiB; the chunk is retained; a second `Serve` 256 KiB at
  256 KiB → hit from the same chunk; after the 4th 256 KiB serve the chunk is
  dropped (fully drained).
- **Cross-chunk serve:** two contiguous ready chunks (off 0 and off 1 MiB); a
  read spanning the boundary → hit, copied across both, leading chunk dropped,
  trailing chunk partially consumed and retained.
- **Full-or-miss:** read whose range extends into a not-yet-ready (or absent)
  chunk → miss `(0,false)`, no chunk mutated (assert `consumed` unchanged).
- **Deep-window arming + slide:** `window=4`, after `threshold` sequential reads
  `Observe` returns 4 offsets; as chunks drain, subsequent `Observe`s return new
  trailing offsets keeping ≤4 in flight; never arms an offset already present.
- **Eviction with retention on seek:** a backward seek drops chunks at/behind the
  new cursor but retains one the cursor lands inside.
- **Threshold gating:** below `threshold` sequential hits, `Observe` arms
  nothing; reset on a non-sequential observation.

### e2e / VM bench (kubevirt VM, real FUSE + netem) — authoritative for the win
- `SeqRead{1,16,64}MiB` over the **WAN netem profile** (50 ms / 100 Mbit,
  `GMOUNTIE_BENCH_TCP=1`) with `readahead_window=4`, chunk = frame size:
  read throughput climbs materially toward the link ceiling versus the
  window=1 baseline.
- **No regression** at the default/LAN and at window=1.
- Tracked on Bencher (the `SeqRead*/wan` series) once released; the bench is the
  acceptance gate, not a committed number (per the no-perf-data-in-docs policy).

## Acceptance criteria

1. `Serve` partial-consumes and serves across contiguous ready chunks; full-or-miss
   semantics; partially-consumed chunks retained; drained/evicted chunks dropped —
   all proven by the unit suite.
2. `Observe` arms a deep window for reads of any size; eviction respects partial
   consume; `threshold` gate intact.
3. On the VM WAN-netem bench, `SeqRead*` throughput with `window=4` improves
   toward the link ceiling with no window=1 / LAN regression.
4. `task lint` + `task test` (unit) pass; the new unit suite covers the
   properties above.
5. No wire/server/fd/cache change; FUSE reads remain full-size.

## On ship (durable docs)

Fold the result into `docs/design/performance.md`: rewrite §5.1 from "deferred
SP5" to the implemented partial-consume readahead (Serve semantics, deep window,
eviction-with-retention), update the §2.5 readahead description and the §6
config defaults (chunk → frame size, window → 4), and note the deferred
buffer-pool follow-up. Update `docs/roadmap.md`'s "Near-term deferred perf
levers" to mark SP5 done. Then prune this transient spec.
