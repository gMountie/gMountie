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

`scripts/perf/profile.sh` is the single source of truth for named netem
profiles. Use it directly or via its thin wrapper scripts:

```bash
# Apply the WAN profile (delay 25ms 5ms jitter, 100Mbit) to loopback
sudo scripts/perf/profile.sh apply wan          # or: sudo scripts/start-slow-loopback.sh [wan] [lo]

# Apply LAN (no shaping — clears any existing qdisc)
sudo scripts/perf/profile.sh apply lan

# Remove shaping when done
sudo scripts/perf/profile.sh clear             # or: sudo scripts/stop-slow-loopback.sh
```

`scripts/start-slow-loopback.sh [profile] [iface]` delegates straight to
`profile.sh apply` (default: `wan lo`). `scripts/stop-slow-loopback.sh [iface]`
delegates to `profile.sh clear` (default: `lo`).

**Always** remove the qdisc when done — silent slowness leaks into later runs.

### Debugging a benchmark

The harness silences the gMountie zap logger inside `TestMain` so the bench
stream stays parseable. Set `GMOUNTIE_BENCH_VERBOSE=1` to restore logs when
diagnosing a hang or unexpected error.

## Continuous tracking (Bencher)

On every **alpha or production release** (not snapshot), a `perf` job in
`.github/workflows/release.yml` runs on the self-hosted `[gmountie-perf]`
runner and uploads results to **Bencher Cloud** (project `gmountie`, testbed
`gmountie-perf-pod`, branch `master`). Each datapoint is identified by the
release commit SHA. Results live off-repo in Bencher; the dashboard is the
over-release overview.

### Network profiles

Two codified profiles, both run over the real loopback TCP transport
(`GMOUNTIE_BENCH_TCP=1`). `scripts/perf/profile.sh` is the single source of
truth for their `tc netem` parameters.

| Profile | Shaping | Effective RTT on loopback |
| ------- | ------- | ------------------------- |
| `lan`   | none    | ~0 ms |
| `wan`   | `delay 25ms 5ms rate 100Mbit` | ~50 ms (packet traverses the qdisc once per direction) |

Benchmarks are tracked as separate series per profile, e.g.
`SeqRead64MiB/lan`, `SeqRead64MiB/wan`.

### Emitted measures

| Measure | Unit | Benchmarks |
| ------- | ---- | ---------- |
| `latency` | ns/op | all |
| `throughput` | MB/s | IO benches (sequential + random) |
| `throughput_pct_of_raw` | % | sequential benches — achieved MB/s as a fraction of the binding `min(disk, link)` ceiling for the profile |
| `_substrate/*` | various | substrate-only series: `disk_seq_read`, `disk_seq_write`, `disk_rand_4k_read_iops`, `disk_rand_4k_write_iops`, `cpu_compute`, `net_rtt_lan`, `net_rtt_wan`, `net_bw_lan`, `net_bw_wan` |

The `_substrate/*` series capture the raw hardware floor (disk, CPU, network)
without gMountie in the path. They make floor drift visible on the dashboard
as its own tracked series — see Drift runbook below.

### Reproduce locally

```bash
# Build just the BMF emitter
task perf:bmf:build

# Full run: substrate probe + lan/wan bench passes + BMF emission
# Needs FUSE + tc (use the kubevirt VM or a Linux host with /dev/fuse).
task perf:ci

# To upload to Bencher instead of writing report.bmf.json locally:
BENCHER=1 BENCHER_PROJECT=gmountie BENCHER_TESTBED=gmountie-perf-pod \
  BENCHER_API_TOKEN=<token> task perf:ci
```

`WORKDIR` must point at a **real block-backed filesystem** (the CI job uses
`/mnt/perf`, a node-local PV). `fio direct=1` and the disk floor probe both
require it — tmpfs or an overlay filesystem will silently produce wrong
numbers. Both the bench data directory (`$TMPDIR`) and the fio probe live
under `WORKDIR`.

### Drift runbook

The dashboard jumped — what to check:

1. **Look at `_substrate/*` first.** If those series moved, the change is
   environmental: disk filling up, a node kernel upgrade, or the runner Pod
   accidentally rescheduled onto a different node. The gMountie code is not
   at fault.
2. **If `_substrate/*` is flat but `throughput` or `latency` moved**, the
   change is in gMountie code. Check the release diff.
3. **If substrate variance is consistently too broad to trust** (the series
   won't settle despite node pinning), replace the Pod runner with a kubevirt
   VM and register it as a new Bencher testbed (`gmountie-perf-vm`). Making
   it a new testbed keeps the substrate change as an explicit series break
   rather than a silent step in the existing data.

## Baselines

| File                                       | Profile               |
| ------------------------------------------ | --------------------- |
| `baseline-2026-05-15-localhost.txt`        | Pre-Phase-3, in-process bufconn, no shaping |
| `baseline-2026-05-15-slow30ms.txt`         | Pre-Phase-3, loopback shaped to 30 ms RTT |
| `baseline-2026-05-15.md`                   | Environment + commit SHA for the above |
