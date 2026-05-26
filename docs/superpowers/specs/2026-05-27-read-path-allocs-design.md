# Read-path allocation fix — frame-buffer pooling + readahead over-issue

**Date:** 2026-05-27
**Status:** approved design, pending spec review → implementation plan
**Branch:** `worktree-read-path-allocs` (off `origin/master` `b5dc764`)
**Scope:** `pkg/server/service/file_streaming.go` (server frame-buffer pool),
`pkg/client/io/readahead.go` (stop the window=1 over-issue). Client-only and
server-only allocation changes — **no wire-protocol, fd-model, or config change.**

## Context — the regression and its root cause

A v1→v2 release comparison plus a VM memory profile established that the
N-deep readahead rewrite (commits `ff2dcb0`→`3a4061b`, landed before the v2
release `0f4152d`) regressed the **default-config** (`readahead_window=1`)
sequential-read path:

- **Bencher (release series), baseline `SeqRead*/lan` throughput** — v1
  (`7df048e`, pre-rewrite, the only pre-rewrite point) is the highest; v2
  (`0f4152d`) and v3 (`5160b14`), both post-rewrite, cluster ~11% below it at
  64 MiB (114.5 → 99.5 / 101.9 MB/s) and ~12% at 16 MiB. Two post-rewrite points
  agreeing against one pre-rewrite point — a consistent, real drop, not one-run
  noise. WAN reads track the same way (masked to ~−6% by the 51 ms RTT).
- **VM `benchstat` (v1 vs v2), `SeqRead{1,16,64}MiB/lan`** — throughput deltas
  are directionally consistent but within the VM's ±14–38% noise (`p`=0.065–0.82);
  the VM cannot resolve a ~14% throughput effect at `count=6 / 3s`.
- **VM `benchstat` allocations — the deterministic, machine-independent signal:**
  `B/op` **+23.9%** geomean, `allocs/op` **+22.5%** geomean, every size
  `p=0.002, n=6`. For a 64 MiB read: 808 MiB → 1045 MiB, 166k → 215k allocs.
- **`pprof -alloc_space` diff (v2 − v1):** dominated by server-side
  `service.(*ReadStreamer).Stream` (+4.3 GB over 20 iters), triggered by client
  `io.(*BackendClient).doPrefetch` (cum +806 MB); the synchronous read path
  (`Read.func1`, `retryableCall`) allocated *less* in v2.

**Mechanism (pinned by static check).** `DefaultFrameSizeBytes = 1<<20` is
identical across releases, and `ReadStreamer.Stream` allocates exactly one
`make([]byte, frameSize)` per `Stream` call (per Read RPC), reused only *within*
a call. So `Stream`'s flat-alloc ratio is the **Read-RPC-count ratio: v2 issues
~34% more Read RPCs than v1 to read the same bytes.** The extra RPCs are wasted
prefetches: the benchmark reads in 256 KiB buffers while `readahead_chunk_bytes`
defaults to 64 KiB, so `Serve` (which misses when `len(dest) > avail`, and a
chunk's `avail` ≤ 64 KiB) **can never hit** — prefetch is pure waste in both
versions. v1 wasted ~1 prefetch then stalled (its single occupied slot blocked
re-arming); v2's window evicts the now-behind unserved chunk and re-arms a fresh
wasted prefetch on essentially every sequential read.

Two independent issues compound: the **client over-issues** wasted prefetch RPCs
(the regression trigger), and the **server allocates a full frame buffer per
RPC** with no pooling (the amplifier — dominant allocator at 71–73% of read
allocation, ~9.6× the data served even in v1).

## Goals

1. Eliminate the wasted-prefetch over-issue at the default `readahead_window=1`,
   returning baseline `SeqRead*` allocations to **≤ v1**.
2. Pool the server-side per-RPC frame buffer so large sequential reads reuse one
   buffer instead of N — a structural win for **all** reads, independent of the
   regression.
3. No behaviour change to write paths, metadata, or the servable small-reader
   readahead path; no protocol/config change.

## Non-goals

- **SP5 (partial-consume readahead).** Making readahead *effective* for readers
  whose buffer exceeds `chunk` (serve sub-ranges, retain the tail, deep window)
  remains SP5. This work only stops the *waste*; readahead stays a genuine no-op
  for big-buffer readers until SP5.
- **Bumping `readahead_chunk_bytes`** to the frame size — overlaps SP5; out of scope.
- **Pooling `doPrefetch`'s chunk buffer** — see Deferred.

## Design

### Fix A — pool the frame buffer (`pkg/server/service/file_streaming.go`)

`ReadStreamer` is a shared instance (one per `RpcFileServerImpl`, constructed at
`controller/file.go:42`); concurrent `Stream` calls each allocate a fresh
`frameSize` buffer today (`file_streaming.go:49`). Since `frameSize` is fixed per
server, add a `sync.Pool` of uniform `frameSize` buffers on the `ReadStreamer`:

- `NewReadStreamer` initialises `bufPool sync.Pool{New: func() any { return make([]byte, frameSize) }}`.
- `Stream` takes `buf := s.bufPool.Get().([]byte)` at entry and `defer s.bufPool.Put(buf)` at exit (`buf` is already length-`frameSize`; it is sliced to `buf[:chunk]` per frame as today).
- **Safety:** `emit` consumes `data` synchronously — `grpc` `stream.Send` marshals
  before returning — which is the same invariant the current "one buffer reused
  across frames" comment relies on. Therefore the buffer is free to return to the
  pool once the final `emit` returns. `sync.Pool` is concurrency-safe, so a shared
  pool across concurrent `Stream` calls is correct.

Effect: a 64 MiB read reuses ~1 buffer instead of ~64; removes the dominant
read-path allocator.

### Fix B — stop the over-issue (`pkg/client/io/readahead.go`)

In `Observe(off, n)`:

- **Unservable-skip (primary).** After the existing behind-cursor eviction, if
  `n > r.chunkSize` return `nil` (arm nothing): a single `chunkSize` chunk can
  never satisfy a read larger than itself, so `Serve` provably cannot hit and any
  prefetch is pure waste. Eviction still drains the stale chunk, so the window
  empties and stays empty for big-buffer readers → zero prefetch RPCs.
- **Single-in-flight (defensive).** Keep `window=1` to ≤1 outstanding chunk and
  do not arm an offset already present in `r.chunks`. This already holds for the
  servable small-reader path; making it explicit guards the invariant.

`BackendClient.Read`'s `for _, off := range h.readahead.Observe(...)` loops
(`backend_grpc.go:550`, `:602`) then spawn no prefetch goroutine for big-buffer
readers → no extra server `Stream` calls.

The servable small-reader path (`n ≤ chunkSize`) is unchanged: it still arms one
chunk ahead, `Serve` hits, and re-arms one-at-a-time.

## Testing — protective properties (not a guessed error shape)

- **Readahead unit** (`pkg/client/io`, testify suite): `Observe` returns no arm
  offsets when `n > chunkSize`; at `window=1` at most one chunk is ever armed and
  an already-present offset is not re-armed; the small-reader path (`n ≤ chunkSize`)
  still arms one-ahead and a subsequent `Serve` at that offset hits. Deterministic,
  no FUSE.
- **Streamer unit** (`pkg/server/service`, testify suite): `testing.AllocsPerRun`
  over **repeated `Stream` calls** (fake `fileRead`/`emit`) asserts the per-call
  frame-buffer allocation amortizes to ~0 once the pool is warm — i.e. allocations
  do **not** scale with the number of `Stream` *calls* (Read RPCs). (Note: the
  current code already reuses one buffer *within* a call; the pool is what removes
  the per-*call* allocation, so the call count — not the frame count — is the axis
  the test must exercise.) Deterministic, no FUSE.
- **Integration confirmation (kubevirt VM, `192.168.11.11`):** re-run the
  `SeqRead64MiB` memory profile (`fix` vs `v1`) and confirm read-path `B/op` is
  **≤ v1**; `benchstat` the throughput for direction. Allocations are deterministic,
  so this is authoritative even at the VM's throughput noise level.

## Deferred

- **`doPrefetch` chunk-buffer pool** (`backend_grpc.go:631`). The buffer is
  *retained* in the readahead window via `Store(off, buf[:written])` until served
  or evicted, so it cannot be returned to a pool at function exit; pooling would
  require an eviction-time return path. After Fix B the prefetch volume is small,
  so the win is minor and the lifecycle trickier. Out of scope for this effort.

## Acceptance criteria

1. Baseline `SeqRead{1,16,64}MiB` allocations (`B/op`, `allocs/op`) return to
   **≤ v1** on the VM memprofile; `benchstat` throughput is no longer below v1
   (within noise or better).
2. Readahead and streamer unit tests prove the protective properties above.
3. The servable small-reader readahead path and all write/metadata paths are
   behaviourally unchanged; no protocol/config change.
4. `task lint` + `task test` pass.

## On ship (durable docs)

Fold into `docs/design/performance.md`: §2.5 (readahead — `window=1`
single-in-flight + unservable-skip; readahead is a no-op when `chunk < read size`
until SP5) and the §2.1/§3 alloc model (server frame-buffer pooling). Fix the
`gmountie` vs `gmountie-tfkojd8g` Bencher project-slug nit in §4.4 + glossary.
Then prune this transient spec.
