# go-fuse vs cgofuse (Linux) benchmark

Gates the deferred "unify Linux onto cgofuse" decision. Decide only if cgofuse
is at parity on metadata-heavy, sequential-throughput, and WAN profiles AND the
cgo build cost is judged acceptable.

## Pre-requisites

- The perf VM (see `docs/design/performance.md §4.3`).
- `libfuse-dev` installed (`apt install libfuse-dev`).
- Go toolchain, `fio`, `iperf3`, `tc`, `ss`, `bencher`.

## 1. Verify both clients build

Run on the perf VM from the repo root. These commands verify build cost and
that the `cgofuse` tag compiles cleanly on Linux before touching the harness.

```bash
# go-fuse (production default — pure Go, no cgo)
CGO_ENABLED=0 go build -o /tmp/gmountie.gofuse ./cmd

# cgofuse (Linux-buildable via the cgofuse tag, requires libfuse-dev)
CGO_ENABLED=1 go build -tags cgofuse -o /tmp/gmountie.cgofuse ./cmd
```

The CLI main package is `./cmd` (not `./cmd/gmountie`). Both commands should
complete without error. Record the build times as the "build cost" gate.

## 2. Run the LAN/WAN matrix — go-fuse pass

The perf harness runs Go benchmarks in-process (not the prebuilt CLI binary
above). The FUSE binding is selected at test-compile time via `FUSE_BINDING`.

```bash
FUSE_BINDING=gofuse \
BENCHER=1 \
BENCHER_PROJECT=gmountie-tfkojd8g \
BENCHER_TESTBED=gmountie-perf-pod \
BENCHER_BRANCH=binding/gofuse \
GIT_HASH=$(git rev-parse HEAD) \
  bash scripts/perf/run.sh
```

Results land in `./perf-out/` (default `WORKDIR`). The Bencher upload uses
branch `binding/gofuse` to keep this series separate from the production
`master` series.

## 3. Run the LAN/WAN matrix — cgofuse pass

```bash
FUSE_BINDING=cgofuse \
BENCHER=1 \
BENCHER_PROJECT=gmountie-tfkojd8g \
BENCHER_TESTBED=gmountie-perf-pod \
BENCHER_BRANCH=binding/cgofuse \
GIT_HASH=$(git rev-parse HEAD) \
  bash scripts/perf/run.sh
```

The harness compiles the test binary with `CGO_ENABLED=1 -tags cgofuse` when
`FUSE_BINDING=cgofuse`. All other harness behaviour (netem profiles, substrate
probe, BMF emission) is unchanged.

Use a different `WORKDIR` if you want to keep both runs' raw outputs side by
side:

```bash
WORKDIR=/tmp/perf-gofuse  FUSE_BINDING=gofuse   bash scripts/perf/run.sh
WORKDIR=/tmp/perf-cgofuse FUSE_BINDING=cgofuse  bash scripts/perf/run.sh
```

## 4. Compare in Bencher

Open the project at `https://bencher.dev/perf/gmountie-tfkojd8g` and use the
**Head** comparison to set:

- Branch A: `binding/gofuse`
- Branch B: `binding/cgofuse`
- Testbed: `gmountie-perf-pod`

Look at `latency` (ns/op) and `throughput` (MB/s) for:

| Profile | Key benchmarks |
|---|---|
| `lan` | `SeqRead64MiB/lan`, `SeqWrite64MiB/lan`, `MetadataStat/lan` |
| `wan` | `SeqRead64MiB/wan`, `SeqWrite64MiB/wan`, `SeqReadOpt64MiB/wan` |

## 5. Decision criteria

Unify Linux onto cgofuse only when **both** gates pass:

1. **Perf parity:** cgofuse `latency` and `throughput` within 5% of gofuse on
   all key benchmarks above (or strictly better).
2. **Build cost acceptable:** the CGO_ENABLED=1 build time and the requirement
   for `libfuse-dev` on the build host are judged acceptable for the CI matrix
   and release pipeline.

If either gate fails, keep Linux on go-fuse and close the branch.
