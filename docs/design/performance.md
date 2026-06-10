# gMountie Performance Design

**Status:** Living document
**Last updated:** 2026-05-27

This document covers the throughput and latency optimizations implemented in
gMountie, the serialization and data-copy model, how performance is measured and
tracked across releases, and the deferred work that remains when the current
improvements become the binding constraint.

The wire protocol, sessions, and reliability primitives are documented in
[architecture.md](architecture.md) and are not repeated here. The client-side
cache and its consistency model are covered in
[caching-and-consistency.md](caching-and-consistency.md).

## 1. Where performance is won and lost

gMountie is a layered pipeline: kernel FUSE boundary → client go-fuse translation
→ gRPC client-stream → wire → gRPC server → loopback filesystem syscall on the
server. Any bottleneck analysis must attribute cost to the right layer.

**The FUSE boundary dominates on metadata.** CPU profiling of
`BenchmarkOpenStatClose` and `BenchmarkLookup` shows the dominant share of CPU time inside
`internal/runtime/syscall/linux.Syscall6` — the FUSE kernel boundary. The gRPC
interceptor stack (request-id injection, metrics, logging) does not appear in
the top 30 non-kernel hot spots. Optimizing the interceptor stack, the per-call
middleware chain, or even the wire-marshal cost has essentially no impact on
metadata latency — the kernel round-trip is the ceiling. This finding rules out
a large class of micro-optimizations that looked promising on a bufconn harness
but would not survive on real hardware.

**Sequential I/O is RTT-bound and bandwidth-bound on WAN.** A 100 Mbit WAN
link (~11.9 MiB/s ceiling) is left largely idle if writes are synchronous (one
RPC in flight at a time) and reads prefetch only one 64 KiB chunk ahead. The
optimizations below attack this problem from the correct layer.

## 2. Implemented optimizations

### 2.1 Streaming Read and Write RPCs

The original `Read`/`Write` were unary RPCs subject to gRPC's default 4 MiB
message ceiling. Phase 3 converted them to streaming:

- **`Read`** is a server-streaming RPC. The client sends one `ReadRequest`;
  the server emits a stream of `ReadFrame` messages, each bounded by
  `server.frame_size_bytes` (default 1 MiB, range 4 KiB–16 MiB). This removes
  the 4 MiB ceiling for large reads and makes frame size tunable independently
  of gRPC message caps.

- **`Write`** is a client-streaming RPC. The client opens a stream and sends
  one or more `WriteFrame` messages; the first frame carries the header fields
  (`volume`, `fd`, `session_id`, `request_id`, `offset`); subsequent frames
  carry only `data`. The server responds once with a unary `WriteReply` after
  the stream closes. This removes the 4 MiB ceiling for large writes and lets
  the client pipeline large writes without waiting for each chunk's
  acknowledgement.

Retry safety is preserved: `request_id` is generated once per logical write
operation and reused across retry attempts, so the server's per-session
idempotency cache can short-circuit a replay without re-executing the write.

### 2.2 Frame-size negotiation via the `Version` RPC

At connect time the client calls `VersionService.Get`. The server's reply
includes `frame_size_bytes` — its configured per-frame ceiling. The client uses
this to cap FUSE `MaxWrite` (which go-fuse uses for both `max_write` and
`max_read`), preventing the kernel from ever submitting a FUSE operation larger
than one frame. Without this cap, the kernel could send a write larger than the
server's frame size, forcing the client to split the frame below the protocol
layer — an unnecessary extra allocation and copy.

### 2.3 ReadDir with attr priming (READDIRPLUS)

`RpcFs.ReadDir` is a server-streaming RPC that lists a directory in batches,
optionally carrying each entry's attributes (`plus=true` — the READDIRPLUS
pattern). The cache decorator primes its attr cache from the listing, so the
kernel's per-entry `LOOKUP`s after a `READDIR` are local hits: an `ls -la`
over N cold files costs **one** round-trip instead of 1+N (`OpenDir` followed
by N `GetAttr`s). Streaming also removes the unary 16 MiB message ceiling that
made very large directories fail with `EIO`.

This supersedes the earlier `RpcFs.Compound` batch-metadata RPC (NFSv4-style
N-ops-in / N-replies-out), which was removed unused — no client path ever
drove it; READDIRPLUS covers the directory-walk case it was built for.

### 2.4 Fused writes: `WriteAndFlush`

Small-file create-write-close is the dominant WAN cost — measured at ~7
critical-path round-trips per `echo x > file` at 100 ms RTT. The breakdown:
`GetAttr`×2, `GetXAttr`×1, `Create`×1, `Write`×1, `Flush`×2. `RELEASE` is
issued asynchronously by the kernel and is not on the critical path.

`WriteAndFlush` fuses the `Write` and the deferred coalesced buffer flush into a
single RPC, eliminating one critical-path RTT on the close tail:

- At `FLUSH` time the client drains the in-memory write coalescer, calls
  `WriteAndFlush(fd, offset, data)`, and receives a `WriteAndFlushReply` with
  the final `Attr` for the file. No separate `Flush` RPC follows.
- The `Create` reply now carries `Attr` for the new file (`CreateReply.attributes`),
  eliminating the post-create `GetAttr` round-trip.
- A clean-handle flush (coalescer empty, nothing written since last flush) issues
  no RPC and returns immediately.

**Impact:** `WriteAndFlush` (one RTT) plus `CreateReply.attributes` (one RTT) remove two round-trips from the critical path, taking small-file create-write-close from ~7 to ~5 critical-path RTTs.

The same reply-carries-attrs pattern now covers the other entry-creating ops
and metadata writes:

- `MkdirReply` and `SymlinkReply` carry the new entry's `Attr`, so `mkdir`
  and `ln -s` are **one** round-trip (no post-create `GetAttr`).
- `RpcFs.SetAttr` applies any combination of mode/owner/size/times in one RPC
  and returns the final attrs. A kernel `SETATTR` used to fan out into up to
  4 serial per-field RPCs (`Truncate`→`Chmod`→`Chown`→`Utimens`) plus a
  trailing `GetAttr` — up to **5 RPCs collapsed to 1**. The per-field RPCs
  were removed from the protocol.
- `WriteAndFlush` and `CopyFileRange` carry a `request_id`, making them
  safely retryable through the same idempotency cache as the other mutations.

`WriteAndFlush` is unary (not streaming) because it only carries the coalesced
tail buffer, which is bounded by `write_coalesce_bytes` (default 1 MiB, well
under the 16 MiB message cap). Large file writes still use the streaming `Write`
path; `WriteAndFlush` carries only the residual tail after streaming completes.

### 2.5 Client readahead and write coalescing

**Readahead:** `pkg/client/io/readahead.go` tracks sequential access patterns
per open file descriptor. After `readahead_threshold` (default 3) strictly
sequential reads it keeps up to `readahead_window` chunks of
`readahead_chunk_bytes` in flight ahead of the consumer, each issued as its own
streaming Read RPC. gRPC multiplexes these over the single HTTP/2 connection, so
a deep window pipelines away the per-fetch round-trip latency — the WAN read
win. Defaults: `readahead_window = 4`, `readahead_chunk_bytes = 1 MiB` (one
server frame).

`Serve` is **partial-consume, cross-chunk, full-or-miss**: it satisfies a read
of any size by copying across one or more contiguous ready chunks, advances a
per-chunk consumed cursor, and **retains partially-consumed chunks** so the next
sequential read hits their tail — a chunk is dropped only once fully drained. A
read not fully covered by ready chunks misses (side-effect-free) and falls to
the synchronous Read, so FUSE reads are never short. A non-sequential `Observe`
evicts chunks at/behind the new cursor (respecting partial consume) and re-arms
from the new position.

This is the SP5 redesign. Previously `Serve` was whole-chunk-or-miss with
one-shot consume, so readahead was a no-op for any reader whose buffer differed
from the chunk size, and deepening the window only wasted RPCs. Now a deep
window of frame-sized fetches saturates the read pipe on a high-RTT link —
measured ≈2× sequential-read throughput at `window=4` vs `window=1` over a
50 ms / 100 Mbit profile, reaching ~70% of the link ceiling. Each retained
chunk holds `readahead_chunk_bytes` until drained, so a deep window costs up to
`window × chunk` per open fd (≈4 MiB at the defaults); pooling that buffer is a
deferred follow-up (§5.1).

**Write coalescing:** `pkg/client/io/coalesce.go` accumulates contiguous small
writes per fd into a single buffer up to `write_coalesce_bytes` (default 1 MiB).
When the buffer overflows, the client flushes via streaming `Write`. At `FLUSH`
time (`WriteAndFlush`) the remaining buffer is drained in one RPC. This reduces
RTTs for the workload of many small sequential writes to the same file (e.g.
stdio `fwrite` with a small buffer, or tar writing fixed-size member chunks).

### 2.6 Kernel writeback cache (opt-in)

With kernel writeback enabled (`fuse.writeback_cache: true`, default `false`),
the kernel buffers writes in the page cache and issues them asynchronously,
with up to `MaxBackground` (64) concurrent FUSE WRITE ops in flight. The
client's per-op `streamingWrite` already handles concurrent calls, so multiple
Write RPCs are pipelined to the server in parallel — this is what fills the WAN
write pipe.

**Why it helps:** with one synchronous WRITE per round-trip, a high-RTT link sits mostly idle between writes. Keeping many WRITE RPCs in flight lets the client approach the link's bandwidth instead of being RTT-bound.

Writeback changes write semantics in ways users must understand:

- **Write errors move to close.** With writeback off, `write(2)` returns an
  error if the RPC fails. With writeback on, errors surface at `fsync` or
  `close`. `WriteAndFlush` carries the error at the `FLUSH` boundary so
  `close()` sees the correct errno.
- **Write visibility is delayed to close.** The kernel buffers writes before
  they reach the server, so a second reader sees the data only after the writer
  calls `close()` (close-to-open consistency, same as NFS). See
  [caching-and-consistency.md §4.5](caching-and-consistency.md) for how the
  Subscribe push-invalidation layer interacts with this.
- **Default is off.** `writeback_cache: false` is the safe default for users
  who need immediate write visibility or synchronous error reporting. Enable it
  explicitly when WAN write throughput is the priority.

### 2.7 Snappy compression (opt-in, default off)

A custom Snappy codec is registered in the server's gRPC encoding registry. It
is applied **per-call and only on streaming Read and Write RPCs**
(`grpc.UseCompressor("snappy")` at the two call sites in
`pkg/client/io/backend_grpc.go`). Metadata RPCs flow uncompressed.

The current default is `rpc.compression: none` (off). **Why:**

On a fast link, Snappy decompression is the bottleneck before the wire is — the
CPU cost of compressing/decompressing ~1 MiB frames back-to-back saturates
before the network does. On a fast link the compression/decompression CPU cost becomes the ceiling before the wire does. For the intended use case (WAN links at 10–100 Mbit where compressible
data would see a real benefit), the default should stay safe and the user who
understands their data's compressibility should opt in with
`rpc.compression: snappy`.

The codec registration is unconditional on the server side (the server honours
whatever compressor the client negotiates); the client-side per-call opt-in
drives whether it engages. Metadata RPCs are unaffected either way.

### 2.8 gRPC keepalive

Client-side HTTP/2 keepalive pings are configured at dial time
(`pkg/client/grpc/factory.go`):

| Parameter | Default | Purpose |
|---|---|---|
| `keepalive.time` | 30 s | How often to ping an idle connection |
| `keepalive.timeout` | 10 s | How long to wait for a pong before closing |
| `keepalive.permit_without_stream` | `true` | Allow pings even when no RPC is active |

These values ensure that half-open TCP connections (e.g. a NAT entry silently
expiring on a WAN link) are detected and broken within `time + timeout = 40 s`.
Without `permit_without_stream`, an idle mount between FUSE operations would
stall indefinitely on the first RPC after a silent disconnect. Both values are
config-driven.

## 3. Serialization and data-copy path

### 3.1 Per-frame copy budget

For a 1 MiB Read frame, user-space touches the payload this many times:

```
SERVER  pread → buf            (kernel→user, unavoidable)
        proto.Marshal          ← 1 alloc + copy (protobuf inherent)
        Snappy compress        ← CPU pass (pooled memory)
── wire ──
CLIENT  Snappy decompress      ← CPU pass (pooled memory)
        proto.Unmarshal        ← 1 alloc + copy (protobuf inherent)
        copy frame into FUSE dest  ← load-bearing
        dest → kernel          (user→kernel, unavoidable)
```

Write is the same in reverse, plus the coalescer's accumulation copies
(`pkg/client/io/coalesce.go`) when small-write coalescing buffers a frame.

That is 4 user-space passes of the payload on top of the two unavoidable kernel
boundaries.

The server recycles the frame-sized read buffer through a `sync.Pool` on the
shared `ReadStreamer` rather than allocating one per Read RPC, so a large
sequential read reuses a single buffer instead of one per frame. This is the
dominant read-path allocation removed; it is safe because `emit` consumes each
frame synchronously (gRPC `Send` marshals before returning).

### 3.2 The `acd2c29` waste-copy fix

`pkg/server/controller/file.go` previously had an unconditional
`copy(buf, out)` after `ReadResult.Bytes(buf)`. The loopback filesystem
(`pathfs → nodefs loopbackFile.Read`) returns a `ReadResultFd` whose
`Bytes(buf)` issues `pread(2)` directly into `buf` and returns `buf[:n]`. The
copy was a self-overlapping memmove of the entire frame for no benefit. It is
now guarded: the copy only fires when `out` does not alias `buf` — which is the
case for an in-memory filesystem that returns its own backing array. This
preserves correctness for that path while eliminating the copy for the common
(loopback) case.

### 3.3 Inherent protobuf-`bytes` copies

`google.golang.org/protobuf` v1.36 has no zero-copy path for `bytes` fields.
Every marshal allocates a fresh wire buffer and copies `Data` into it; every
unmarshal allocates a fresh `[]byte` for `Data` and copies the payload into it.

A micro-benchmark (`pkg/proto/serialization_copy_bench_test.go`) confirms the allocations: marshal does one allocation + copy of the payload; unmarshal does two. The allocation sizes track the payload size — there is no pooling or reuse for `bytes` fields.

These allocations and copies are inherent to the protobuf-`bytes` contract and
cannot be removed with the standard codec.

### 3.4 Deferred: zero-copy `CodecV2` marshaling

grpc-go ≥ v1.66 (the project is on v1.81) exposes a `mem.BufferSlice` /
`CodecV2` interface. A custom codec could thread the FUSE-provided `dest` buffer
(on Read) or the kernel write buffer (on Write) through to the transport without
the intermediate `proto.Marshal`/`Unmarshal` allocation — eliminating the two
inherent protobuf copies from the budget above.

This is a significant change: it requires hand-rolling the framing for the data
payload, separate from the protobuf-encoded header fields, and managing
buffer-pool lifetimes against the transport. The Snappy compressor would also
need to migrate to the `CodecV2` buffer model to avoid reintroducing a copy.

**Decision: deferred.** The trigger to revisit is when the Bencher series flags
serialization overhead as the dominant cost on a real-transport (`lan` or `wan`)
profile. On metadata ops the proto copy cost is below measurement noise (the
FUSE boundary dominates). For large sequential I/O over a fast link it is the
most likely next bottleneck once the network floor stops binding — that is when
to invest.

## 4. Benchmarking and continuous tracking

### 4.1 Running the harness locally

The benchmark suite lives in `test/e2e/perf/`. Each `Benchmark*` builds its own
in-process server + FUSE mount and tears it down on cleanup.

```bash
# Install benchstat (one-time)
task perf:install

# Capture a run (defaults: COUNT=5, BENCHTIME=10s)
task perf:bench OUT=perf-out/before.txt

# Make a change, then measure again
task perf:bench OUT=perf-out/after.txt

# Compare with statistical significance
task perf:diff BEFORE=perf-out/before.txt AFTER=perf-out/after.txt
```

**Variants:**

| Task | Transport | Notes |
|---|---|---|
| `perf:bench` | in-process bufconn | Fast; no `tc netem` effect; good for relative deltas |
| `perf:bench:tcp` | loopback TCP (`GMOUNTIE_BENCH_TCP=1`) | Real socket; netem shaping bites here |
| `perf:bench:cache` | in-process bufconn with cache on | `GMOUNTIE_BENCH_CACHE=1` |

The harness requires Linux + FUSE3 (`/dev/fuse`). Use the kubevirt VM at
`192.168.11.11` in sandboxed environments. Set `GMOUNTIE_BENCH_VERBOSE=1` to
restore gMountie's zap logger output (silenced by default so the output stream
stays `benchstat`-parseable).

**Variables:**

- `COUNT` — runs per benchmark (default 5; benchstat wants ≥6 for tight CIs)
- `BENCHTIME` — per-iteration budget (default `10s`; raise to `20s` for
  high-latency runs where each op is slow)
- `OUT` — output path (default: timestamped under `perf-out/`, which is gitignored)

### 4.2 Network profiles and `netem` shaping

`scripts/perf/profile.sh` is the single source of truth for `tc netem` profiles.
Two profiles are defined:

| Profile | Shaping | Effective RTT |
|---|---|---|
| `lan` | none | ~0 ms |
| `wan` | `delay 25ms 5ms rate 100Mbit` | ~50 ms |

```bash
sudo scripts/perf/profile.sh apply wan   # or: sudo scripts/start-slow-loopback.sh
sudo scripts/perf/profile.sh apply lan
sudo scripts/perf/profile.sh clear       # or: sudo scripts/stop-slow-loopback.sh
```

Always clear the qdisc when done — silent slowness leaks into later runs.

The wrapper scripts `scripts/start-slow-loopback.sh [profile] [iface]` and
`scripts/stop-slow-loopback.sh [iface]` delegate to `profile.sh`; the defaults
are `wan lo` and `lo` respectively.

### 4.3 What the harness measures

| Benchmark | Measures |
|---|---|
| `BenchmarkSeqRead{1,16,64}MiB` | Sequential read throughput (`b.SetBytes` → MB/s) |
| `BenchmarkSeqWrite{1,16,64}MiB` | Sequential write throughput |
| `BenchmarkRandomRead4KiB` | 4 KiB random `ReadAt` from a 64 MiB file |
| `BenchmarkRandomWrite4KiB` | 4 KiB random `WriteAt` |
| `BenchmarkOpenStatClose` | `os.Stat` round-trip (Lookup + GetAttr) |
| `BenchmarkReaddir100` | `os.ReadDir` on a 100-entry directory |
| `BenchmarkLookup` | `os.Lstat` round-trip |

`benchstat` reports `sec/op`, `B/s` (for throughput benches), `B/op`,
`allocs/op` with confidence intervals and a `p` column. `p > 0.05` typically
means the delta is noise.

### 4.4 Continuous tracking with Bencher

On every **alpha or production release** (not snapshot), a `perf` job in
`.github/workflows/release.yml` runs on the self-hosted `[gmountie-perf]`
runner (a single node-pinned Pod in the project's k8s cluster) and uploads
results to **Bencher Cloud**.

**Why release-gated:** performance signal is consumed per-release ("what
changed between v0.5 and v0.6?"), not per-commit. Release cadence keeps the
series sparse but meaningful. The runner Pod is fixed to one node with
dedicated CPUs, local storage, and a node taint so the hardware floor never
silently rotates between runs.

**Bencher data model:**

- **Project:** `gmountie-tfkojd8g`
- **Testbed:** `gmountie-perf-pod` — the fixed runner Pod. If the substrate is
  replaced (e.g. Pod → kubevirt VM), a **new testbed** is registered so the
  substrate change is an explicit series break, not a silent step in the data.
- **Branch:** `master` — passed explicitly to `bencher run` (`--branch master`)
  so the over-time series accumulates on one Branch. The current release tag
  annotates each datapoint via `--hash $GITHUB_SHA`.
- **Benchmark names:** suffixed by profile, e.g. `SeqRead64MiB/lan`,
  `SeqRead64MiB/wan`.

**Emitted measures per benchmark:**

| Measure | Unit | Notes |
|---|---|---|
| `latency` | ns/op | All benchmarks |
| `throughput` | MB/s | I/O benchmarks (sequential + random) |
| `throughput_pct_of_raw` | % | Achieved MB/s as a fraction of the `min(disk, link)` ceiling for the profile |

**Substrate fingerprint series (`_substrate/*`):** the same job also measures
the hardware floor (CPU compute, disk sequential/random read/write, loopback
RTT and bandwidth per profile) without gMountie in the path and uploads these as
separate benchmark series. This is what delivers "detect substrate drift":
`throughput_pct_of_raw` self-normalizes against a moving floor, so the raw
substrate series is needed to see when the floor itself has moved.

Available `_substrate/*` series: `disk_seq_read`, `disk_seq_write`,
`disk_rand_4k_read_iops`, `disk_rand_4k_write_iops`, `cpu_compute`,
`net_rtt_lan`, `net_rtt_wan`, `net_bw_lan`, `net_bw_wan`.

**Regression policy:** alert only (advisory). The `perf` job runs
post-release-tag; Bencher sends an alert on threshold breach, but the release
stands. The perf job never blocks a release.

### 4.5 Declarative dashboard plots

The Bencher dashboard *plots* (the pinned charts on the project's Perf page) are
defined declaratively in **`scripts/perf/plots.yaml`** and reconciled by
`perfbmf plots sync`. This exists because `bencher plot update` cannot change a
plot's benchmark or measure set — and new benchmarks never auto-join a plot — so
without a reconciler the dashboard silently drifts from reality each time a
benchmark is added.

The spec references benchmarks by **name glob** (`path.Match`, `*` does not cross
`/`), e.g. `Seq*MiB/lan`, so a future `SeqReadOpt128MiB/lan` is folded into the
right plot on the next sync with no manual UUID edits. Document order is the
dashboard index; the plot title is the match key.

- `task perf:plots:diff` — dry-run; prints the plan (read-only, no token needed).
- `task perf:plots:sync` — converge the live dashboard (needs `$BENCHER_API_TOKEN`;
  `PRUNE=1` also deletes live plots absent from the spec — off by default so
  ad-hoc plots made in the web UI survive).

The diff/planner is a pure function in `test/e2e/perf/bmf/plotsync.go`
(unit-tested); the executor shells out to the `bencher` CLI. A plot whose
benchmark/measure/branch/testbed/x-axis or boundary flags changed is recreated
(create-new-then-delete-old); only title/window/index changes use `plot update`.

**Measure units** are pinned in the same spec, under a `measures:` map keyed by
measure name or slug (e.g. `throughput: "megabytes / second (MB/s)"`). Bencher
auto-creates each measure from the BMF report with the placeholder unit
`"Measure (units)"`, so without this the axis labels are wrong (the `throughput`
axis read `operations / second` despite every value being MB/s). `plots sync`
reconciles these alongside the plots via `bencher measure update`; a spec key
matching no live measure is a hard error.

### 4.6 Running the full CI pipeline locally

```bash
# Build the BMF emitter
task perf:bmf:build

# Full run: substrate probe + lan/wan bench passes + BMF emission
# Requires FUSE + tc (use the kubevirt VM or a Linux host with /dev/fuse).
task perf:ci

# Upload to Bencher instead of writing report.bmf.json locally:
BENCHER=1 BENCHER_PROJECT=gmountie BENCHER_TESTBED=gmountie-perf-pod \
  BENCHER_API_TOKEN=<token> task perf:ci
```

`WORKDIR` must point at a real block-backed filesystem (`/mnt/perf` in CI).
`fio direct=1` and the disk floor probe both require it — tmpfs and overlay
filesystems produce wrong numbers.

### 4.6 Drift runbook

The dashboard jumped — what to check:

1. **Look at `_substrate/*` first.** If those series moved, the change is
   environmental: disk filling up, a node kernel upgrade, or the runner Pod
   accidentally rescheduled onto a different node. The gMountie code is not
   at fault.
2. **If `_substrate/*` is flat but `throughput` or `latency` moved,** the
   change is in gMountie code. Check the release diff.
3. **If substrate variance is consistently too broad to trust** (the series
   won't settle despite node pinning), replace the Pod runner with a kubevirt
   VM and register it as a new Bencher testbed (`gmountie-perf-vm`).

## 5. Deferred / future work

### 5.1 Readahead prefetch-buffer pooling

The partial-consume readahead (§2.5) retains each in-flight/ready chunk until it
is fully drained or evicted, so a deep window holds up to `readahead_window ×
readahead_chunk_bytes` per open fd, unpooled (≈4 MiB at the defaults). Each
chunk buffer is allocated per prefetch (`doPrefetch`) and freed on
drain/eviction. If the Bencher read-path allocation series flags this, pool
them: a `sync.Pool` of chunk-sized slices on `Readahead`, taken in `Store` and
returned at drain/eviction. The eviction-time return path is the non-trivial
piece, so this is deferred until the measured cost justifies it.

### 5.2 Zero-copy `CodecV2` marshaling (serialization win)

See §3.4 above. Deferred until the Bencher series shows serialization as the
dominant cost on a real-transport profile.

## 6. Configuration reference

The performance-relevant knobs, their defaults, and where they live:

**Client — RPC (`rpc.*` / `GMOUNTIE_RPC_*`):**

| Key | Default | Notes |
|---|---|---|
| `rpc.readahead_chunk_bytes` | `1048576` (1 MiB) | Size of each prefetched chunk (one server frame) |
| `rpc.readahead_threshold` | `3` | Strictly-sequential reads before arming prefetch |
| `rpc.readahead_window` | `4` | Chunks kept prefetched/in-flight; deepen on high-BDP links (range 1–64) |
| `rpc.write_coalesce_bytes` | `1048576` (1 MiB) | Per-fd small-write coalescing buffer cap |
| `rpc.compression` | `none` | `none` or `snappy`; default off — see §2.7 |
| `rpc.keepalive.time` | `30s` | Ping interval on idle connection |
| `rpc.keepalive.timeout` | `10s` | Pong deadline |
| `rpc.keepalive.permit_without_stream` | `true` | Ping even when no RPC is active |

**Client — FUSE (`fuse.*` / `GMOUNTIE_FUSE_*`):**

| Key | Default | Notes |
|---|---|---|
| `fuse.writeback_cache` | `false` | Enable kernel writeback for WAN write throughput; changes write-error and write-visibility semantics — see §2.6 |

**Server (`server.*` / `GMOUNTIE_SERVER_*`):**

| Key | Default | Notes |
|---|---|---|
| `server.frame_size_bytes` | `1048576` (1 MiB) | Per-frame ceiling for streaming Read/Write; negotiated to the client via the `Version` RPC |

## 7. Glossary

- **Frame size** — the maximum payload (bytes) of a single `ReadFrame` or
  `WriteFrame`. Controlled by `server.frame_size_bytes`; negotiated to the client
  via `VersionService.Get` to cap FUSE `MaxWrite`.

- **Readahead window** — the number of `readahead_chunk_bytes` chunks the client
  keeps prefetched or in-flight ahead of the sequential read cursor. Controlled by
  `rpc.readahead_window` (default 4). A window of 1 = the legacy single-chunk-ahead
  behaviour.

- **Write coalescing** — the per-fd client-side buffer that accumulates contiguous
  small writes up to `rpc.write_coalesce_bytes` before issuing a `Write` RPC.
  Drained at `FLUSH` time via `WriteAndFlush`.

- **`WriteAndFlush`** — a unary RPC on `RpcFile` that fuses the coalesced write
  buffer drain and the FUSE FLUSH into a single network round-trip. The close-tail
  win for small-file WAN workloads.

- **Writeback cache** — FUSE `CAP_WRITEBACK_CACHE`. When enabled, the kernel
  buffers writes and issues them asynchronously. Enabled by `fuse.writeback_cache:
  true`. Default off.

- **Snappy** — the custom gRPC compressor registered under the name `"snappy"`.
  Applied per-call on streaming `Read`/`Write` only when `rpc.compression:
  snappy`. Not applied to metadata RPCs.

- **Bencher** — the continuous benchmarking service (Bencher Cloud) used to store
  the per-release performance time series for the project (`gmountie-tfkojd8g` project,
  `gmountie-perf-pod` testbed, `master` branch).

- **Substrate fingerprint** — the set of `_substrate/*` Bencher benchmark series
  that capture the raw hardware floor (CPU, disk, network) without gMountie in the
  path. Used to detect runner drift before attributing a dashboard jump to code.

- **BMF (Bencher Metric Format)** — the JSON format consumed by the
  `bencher run --adapter json` ingestion path. The project's BMF emitter lives in
  `test/e2e/perf/cmd/perfbmf/`.

- **SP5** — the implemented partial-consume readahead redesign (the WAN read win).
  See §2.5.
