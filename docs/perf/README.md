# gMountie perf harness

Go-bench-shaped performance harness for gMountie. The benchmarks live under
[`test/e2e/perf/`](../../test/e2e/perf) and are driven by `go test -bench`
through Taskfile shortcuts. We capture results as plain text files so they can
be diffed with [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat),
which compares two runs with statistical significance (geomean + p-value)
rather than eyeballing markdown tables.

## What's in the harness

Each `Benchmark*` builds its own in-process server + FUSE mount via
`test/e2e/utils` and tears it down on cleanup, so workloads are isolated and
the harness can be re-run unattended.

| Bench                          | What it measures |
| ------------------------------ | ---------------- |
| `BenchmarkSeqRead{1,16,64}MiB` | Sequential read throughput (MB/s via `b.SetBytes`) |
| `BenchmarkSeqWrite{1,16,64}MiB`| Sequential write throughput |
| `BenchmarkRandomRead4KiB`      | 4 KiB random `ReadAt` from a 64 MiB file (deterministic seed) |
| `BenchmarkRandomWrite4KiB`     | 4 KiB random `WriteAt` (deterministic seed) |
| `BenchmarkOpenStatClose`       | `os.Stat` round-trip (Lookup+Getattr) |
| `BenchmarkReaddir100`          | `os.ReadDir` on a 100-entry dir |
| `BenchmarkLookup`              | `os.Lstat` round-trip |

The 1 GiB sequential size from the original spec was capped at 64 MiB to keep
each bench tractable on the test VM (3.8 GiB RAM, 30 ms loopback profile).
64 MiB is comfortably larger than per-connection windows / buffers and any
in-flight caches we expect to add later.

The fio-driven `test/e2e/fs/io_bench_test.go` is left untouched as a smoke
test, but its output isn't `benchstat`-parseable, so it isn't load-bearing.

## Requirements

- Linux + FUSE3 (`/dev/fuse`). Sandboxed environments without `/dev/fuse`
  cannot run the harness — use the kubevirt VM at `192.168.11.11`.
- `task perf:install` to drop `benchstat` into `$GOPATH/bin`.

## Workflow

```bash
# One-time
task perf:install

# Capture a run (defaults: COUNT=5, BENCHTIME=10s, OUT=docs/perf/bench-<ts>.txt)
task perf:bench OUT=docs/perf/before.txt

# ... change code ...

task perf:bench OUT=docs/perf/after.txt

# Compare
task perf:diff BEFORE=docs/perf/before.txt AFTER=docs/perf/after.txt
```

`benchstat` reports `sec/op`, `B/s` (when `b.SetBytes` is set), `B/op`,
`allocs/op` with confidence intervals and a `p` column for statistical
significance. p > 0.05 usually means the delta is noise.

### Useful variables

- `COUNT` — number of runs (default 5; benchstat wants ≥6 for tight CIs but 5
  is the practical sweet spot on the VM).
- `BENCHTIME` — per-iter budget (default `10s`; bump to `20s` for high-latency
  scenarios where each op is slow).
- `OUT` — output file path. Default uses a timestamp so concurrent runs don't
  clobber each other.

### Slow loopback

`scripts/start-slow-loopback.sh` / `stop-slow-loopback.sh` (hardcoded at
`delay 10ms rate 1000Mbit`) shape the loopback interface via `tc netem`. For
custom delays — e.g. the 30 ms profile used in the Phase 3 baseline — invoke
`tc` directly:

```bash
sudo tc qdisc add dev lo root netem delay 30ms
task perf:bench OUT=docs/perf/slow30ms.txt COUNT=3 BENCHTIME=20s
sudo tc qdisc del dev lo root
```

**Always** remove the qdisc when done — silent slowness leaks into later runs.

### Debugging a benchmark

The harness silences the gMountie zap logger inside `TestMain` so the bench
stream stays parseable. Set `GMOUNTIE_BENCH_VERBOSE=1` to restore logs when
diagnosing a hang or unexpected error.

## Baselines

| File                                       | Profile               |
| ------------------------------------------ | --------------------- |
| `baseline-2026-05-15-localhost.txt`        | Pre-Phase-3, in-process bufconn, no shaping |
| `baseline-2026-05-15-slow30ms.txt`         | Pre-Phase-3, loopback shaped to 30 ms RTT |
| `baseline-2026-05-15.md`                   | Environment + commit SHA for the above |
