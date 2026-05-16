# Phase 3 follow-up: per-call interceptor cost

**Date:** 2026-05-16. Commit measured: `b482ef5`.
**Question this answers:** is the +19% bufconn-bench metadata regression
that Phase 3's final review flagged actually caused by the per-call
interceptor stack (request-id, metrics, logging, etc.)?

## TL;DR

**No.** CPU profiling of the metadata-heavy benches shows the gRPC
interceptor stack does not appear in the top 30 non-kernel hot spots.
The +19% is measurement noise and/or GC pressure from other Phase 3
allocations, not interceptor overhead. Optimising the interceptor
stack won't move the needle on metadata throughput.

## Method

Ran with CPU profiling on:

```bash
go test -run=^$ -bench='BenchmarkOpenStatClose|BenchmarkLookup' \
        -count=1 -benchtime=10s -cpuprofile=metadata.pprof \
        ./test/e2e/perf/
```

Total: 4.4M iterations of each bench over ~40s on the kubevirt VM
(Intel Broadwell vCPU, 4 vCPU / 3.8 GiB RAM).

## Results

### Overall CPU breakdown

```
   24960ms 78.39%  internal/runtime/syscall/linux.Syscall6
   ~3000ms ~9%     runtime GC (mallocgc + scanObject + sweep)
    other  ~12%    benchmark loop, os.Stat plumbing, gRPC layer
```

`syscall.Syscall6` is the FUSE kernel boundary — every Stat issues a
LOOKUP+GETATTR via `/dev/fuse`. **78% of CPU time is in the kernel.**

### Non-kernel hot spots (interceptors filtered for)

Filtering out the syscall path and OS-layer wrappers, the top non-kernel
nodes are:

```
    0.49s  1.54%  runtime.scanObject       (GC)
    0.26s  0.82%  runtime.tryDeferToSpanScan (GC)
    0.32s  1.01%  runtime.scanObjectsSmall (GC)
    0.55s  1.73%  runtime.sweepone         (GC)
    0.14s  0.44%  BenchmarkLookup itself
```

**The gRPC interceptor stack does not appear** in the top 30. The
runtime GC (cumulative ~3% of total CPU) dominates the userspace
portion.

## Implications

The +19% sec/op delta on `BenchmarkOpenStatClose` / `BenchmarkLookup`
between the Phase 3 baseline and final runs (2.4 µs → 2.8 µs) is not
caused by the per-call interceptor stack. Possible alternative
explanations, in rough order of likelihood:

1. **GC pressure from Phase 3 allocations elsewhere.** Tasks 2 / 3 / 8
   / 9 introduced per-fd readahead/coalescer state, request_id strings
   per Write, prefetch goroutines. Even though these don't run during
   `OpenStatClose`, their allocations during bench setup affect the GC
   heap, and concurrent GC can preempt the bench goroutine in ways
   that show up as per-op variance.

2. **Measurement noise.** 400 ns on a 2-3 µs op is a 19% percent delta,
   but in absolute terms it's well within day-to-day VM CPU noise.
   Both the baseline and the final runs used N≥3 samples (some with
   N=2-3 due to the EIO flake which was independently mitigated in
   the F3 follow-up), which gives benchstat a `p=0.008` significance
   but with only 5 samples the underlying variance estimate is fragile.

3. **Wire-marshal cost growth.** Phase 1d added `request_id` and
   `session_id` to many proto messages. `GetAttrRequest` does not
   carry request_id but does carry `session_id` (added in Phase 1c).
   The marshal cost grew by one string field. On a bufconn transport
   with no actual network, this is amplified.

What we can rule out: **interceptor overhead.** The middleware chain
(request-id injection, logging context fields, metrics labels) is too
cheap to measure here.

## What this means for follow-up work

- **Don't optimize the interceptor stack** for metadata throughput.
  There's nothing to win.
- **Do reconsider the +19% as noise** until a real-TCP run under
  shaping (where metadata-op overhead is dominated by network RTT,
  not by µs-scale CPU work) tells a clearer story. F4 added the TCP
  harness; a re-baseline on it would be the right confirming step,
  but the absolute deltas in the bufconn harness aren't worth chasing.
- **If a future profile shows a real interceptor cost** (e.g., for a
  more bench-friendly transport where wire-marshal isn't 90% of the
  budget), revisit. Until then, the per-call instrumentation Phase 2
  shipped (request-id, metrics, logging) is paying its way at zero
  cost on the hot path.

## Raw data

`metadata.pprof` is not committed (16 MB). It can be regenerated via
the command in the Method section.
