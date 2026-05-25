# Data copies on the gRPC serialization path

**Date:** 2026-05-25. Commits: `acd2c29` (fix), `96aa11a` (benchmark).
**Question this answers:** does the Go gRPC layer copy file data a lot
during serialization / deserialization on the Read and Write path?

## TL;DR

**Yes.** A 1 MiB Read frame is passed over the payload **4 times in user
space** on top of the two unavoidable kernel boundaries. Two of those are
inherent to protobuf-go (`bytes` fields have no zero-copy path); two are
gMountie's own. One was pure waste and has been removed (`acd2c29`). The
rest are either inherent or load-bearing for the streaming design.

## The inherent cost: protobuf-go copies `bytes` twice

`google.golang.org/protobuf` v1.36 has **no zero-copy path for `bytes`
fields** (`ReadFrame.Data` / `WriteFrame.Data`). Proven with a micro-bench
(`pkg/proto/serialization_copy_bench_test.go`, 1 MiB payload):

```
BenchmarkReadFrameMarshal-12      1056773 B/op   1 allocs/op
BenchmarkReadFrameUnmarshal-12    1048661 B/op   2 allocs/op
BenchmarkSnappyRoundtrip-12        119190 B/op   7 allocs/op
```

- **Marshal** allocates a fresh ~1 MiB wire buffer and copies `Data` into it.
- **Unmarshal** allocates a fresh `[]byte` for `Data` (1 MiB = `1<<20` =
  1048576, plus the struct) and copies the payload into it.
- **Snappy** (the registered `encoding.RegisterCompressor` codec in
  `pkg/server/grpc/snappy`) shows low heap because it pools 64 KiB block
  buffers and reuses writers/readers — but it still memmoves the full
  payload through those blocks **once per direction**. Low allocs ≠ free;
  it's two extra CPU passes over the payload.

This benchmark is a unit-level micro-bench, not part of the FUSE harness
under `test/e2e/perf/`. It's pure proto + codec, so it runs anywhere (no
`/dev/fuse` needed) and isolates serialization cost from syscall cost.

## Per-frame copy budget (1 MiB Read)

```
SERVER  Pread → buf            (kernel→user, unavoidable)
        file.go self-copy      ← WAS WASTE — removed in acd2c29
        proto.Marshal          ← 1 alloc + copy (protobuf, inherent)
        snappy compress        ← CPU pass (pooled mem)
── wire ──
CLIENT  snappy decompress      ← CPU pass (pooled mem)
        proto.Unmarshal        ← 1 alloc + copy (protobuf, inherent)
        backend_grpc.go:527    ← copy frame data into FUSE dest
        dest → kernel          (unavoidable)
```

Write is the same in reverse, plus the coalescer's accumulation copies
(`pkg/client/io/coalesce.go`) when small-write coalescing is enabled.

## What was fixed (`acd2c29`)

`pkg/server/controller/file.go` had an unconditional `copy(buf, out)` after
`ReadResult.Bytes(buf)`. The loopback FS we actually serve (pathfs →
nodefs `loopbackFile.Read`) returns a `ReadResultFd` whose `Bytes(buf)`
Preads **straight into buf** and returns `buf[:n]` — so `out` already
aliases `buf`, and the copy was a self-overlapping memmove of the whole
frame on every Read for no gain. Now guarded to copy only when `out` does
not alias `buf` (e.g. an in-memory FS handing back its own backing array),
which preserves correctness for that path.

## The remaining lever: a zero-copy codecV2 (not done)

The two protobuf copies cannot be removed with the standard codec — they
follow from `[]byte` value semantics. The only way to eliminate them is to
stop round-tripping the payload through a protobuf `bytes` field:

- grpc-go ≥ v1.66 (we're on v1.81) exposes the `mem.BufferSlice` /
  `CodecV2` interface. A custom codec could hand the FUSE-provided `dest`
  buffer (read) or the kernel write buffer through to the transport
  without the intermediate `proto.Marshal`/`Unmarshal` allocation.
- This is a **much larger change** — a hand-rolled framing for the data
  payload, separate from the protobuf-encoded header fields, plus
  buffer-pool lifetime management against the transport. The registered
  snappy compressor would also need to move to the codecV2 buffer model
  to avoid reintroducing a copy.

**Decision: deferred.** Worth it only if the Bencher perf series flags
serialization as the dominant cost on a real-transport profile. On
metadata ops it isn't even measurable (see
[`interceptor-cost-2026-05-16.md`](interceptor-cost-2026-05-16.md): 78% of
CPU is the FUSE syscall boundary). For large sequential IO over a fast
link it's the most likely next bottleneck once the network floor stops
binding — that's the trigger to revisit.
