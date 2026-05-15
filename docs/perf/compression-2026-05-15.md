# Compression decision — 2026-05-15

## Context

Phase 3 Task 5 set out to stop applying Snappy compression to every RPC
(metadata ops pay codec CPU for ~no benefit) and make it opt-in per
call on the streaming `Read`/`Write` RPCs only.

On inspection, the codebase already matched that target shape:

- `pkg/server/grpc/snappy/snappy.go` registers `"snappy"` with the gRPC
  encoding registry at package init (`encoding.RegisterCompressor`).
  That is the only thing the server does — it never set a global
  default-apply.
- `pkg/server/grpc/server.go` blank-imports the snappy and gzip codec
  packages so the names are registered, but `getOptions()` does not
  install `grpc.ForceServerCodec`, `grpc.RPCCompressor`, or any other
  server-side default compression option.
- `pkg/client/grpc/factory.go` and `pkg/client/grpc/client.go` do not
  install `grpc.WithDefaultCallOptions(grpc.UseCompressor("snappy"))`
  (nor any equivalent) on the dial options.
- `pkg/client/io/file.go` is the only caller of `grpc.UseCompressor`,
  and it passes `grpc.UseCompressor(snappy.Name)` *only* on the
  streaming `Read` (line 64) and streaming `Write` (line 118) RPCs.

Confirmed via:

```
grep -rn "ForceServerCodec\|RPCCompressor\|WithDefaultCallOptions\|ForceCodec\|UseCompressor" \
    --include='*.go' .
```

The original "added compression" commit (`a9eee5f`, 2024-10-23) only
registered the codec name and applied `UseCompressor` per-call at the
two file-IO call sites; it never wired a global default. Tasks 2 and 3
of Phase 3 (streaming `Read`/`Write`) carried that per-call call option
forward.

So the code-modification portion of Task 5 is a no-op against the
current tree: there is no global default-apply to remove, and the
per-call opt-in on the streaming RPCs is already in place. Metadata
RPCs (`GetAttr`, `OpenDir`, `Access`, `StatFs`, `GetXAttr`, `Compound`,
session/version handshakes, etc.) already flow uncompressed.

## Workload

The perf harness in `test/e2e/perf/` runs the server, client, and FUSE
mount inside one Go process against a `bufconn` listener (see
`test/e2e/utils/app.go:91`). gRPC frames never traverse a real socket,
so wire-codec CPU is exercised but wire-codec *throughput* is not — the
"wire" is a Go channel copy. This is documented in
`docs/perf/baseline-2026-05-15.md`.

The baseline runs from Task 1 (`baseline-2026-05-15-localhost.txt`,
`baseline-2026-05-15-slow30ms.txt`) were captured with Snappy already
on streaming `Read`/`Write` and nothing else, which is exactly the
target state of Task 5. Those numbers therefore *are* the Snappy
"after" measurement for this task; there is no separate before to diff
against.

## Snappy (per-call on streaming Read/Write)

Baseline numbers from Task 1 stand in as the Snappy result, since the
codec wiring did not change:

- `BenchmarkOpenStatClose`: ~2.4 µs/op, 256 B/op, 2 allocs/op
- `BenchmarkLookup`: ~2.4 µs/op, 272 B/op, 2 allocs/op
- `BenchmarkReaddir100`: ~27 ms/op (server-side `Readdir` over 100
  entries — pre-Compound, see Task 4)
- Streaming `Read`/`Write` throughput: see
  `docs/perf/baseline-2026-05-15-localhost.txt` (sequential and random
  IO benchmarks)

The bufconn-bound harness does not give a meaningful signal on
wire-codec CPU at the data-plane level — the "transport" is just a
buffered Go channel — so these numbers cannot honestly be cited as a
codec-efficiency measurement. They confirm only that the per-call
wiring works and the rest of the suite is green.

## zstd level 1

Deferred. Justification:

1. **No measurable signal on the current harness.** Swapping to zstd
   would change the codec called inside the gRPC send/recv path, but
   the transport itself is a `bufconn.Listener`, not TCP, so the
   compressed bytes are never written to a socket. Any throughput
   delta would be dominated by CPU on a path that does not exist in
   production.

2. **No code change to ship either way.** If Snappy still wins on a
   future real-TCP harness, the result is "keep what we have." If
   zstd wins, the swap is mechanical (`go-grpc-compression/zstd`
   import + change the string in the two `UseCompressor` call sites).
   Running a 20-minute bench whose result has a known confounder, with
   no shipping decision riding on it, is not a good use of bench
   budget.

3. **Real-TCP perf harness is already in the Phase 3 roadmap.** A
   future task that moves the perf rig off bufconn onto a real TCP
   socket (or netem-shaped two-process setup) is the natural place to
   actually compare codecs. See the follow-up below.

## Decision

**Keep Snappy.** The per-call wiring on streaming `Read`/`Write` is
already correct; the rest of the RPC surface already flows
uncompressed. There is no evidence on the current harness that
switching codecs would be a net win, and changing the codec without
evidence is churn.

The Phase 3 plan accepts this outcome explicitly: "If it's not a clear
win at first pass, flag as a follow-up."

## Follow-up

- **Real-TCP perf harness.** Once the perf harness runs server and
  client in separate processes over a real loopback (or netem-shaped)
  TCP socket, re-run the Snappy-vs-zstd comparison. Only then will the
  CPU-vs-bytes-on-wire tradeoff actually be visible to the benchmark.
  Track separately from this task.
- **Per-frame compression skip.** Worth considering on the streaming
  paths: tiny final frames (terminal status frame on `Read`, partial
  trailing frame on `Write`) probably do not justify a codec pass. Low
  priority; revisit if profiling shows codec time as a hot spot under
  load.
