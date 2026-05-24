# Release-Gated Perf Tracking (Bencher) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the in-repo half of the perf-tracking pipeline: convert the existing `test/e2e/perf` benchmark output plus a substrate fingerprint into Bencher Metric Format (BMF) JSON and upload it from a release-gated CI job, over codified LAN/WAN profiles.

**Architecture:** A small, fully-unit-tested Go library (`test/e2e/perf/bmf`) does all parsing and normalization; a thin CLI (`cmd/perfbmf`) exposes it. Shell glue (`scripts/perf/`) runs the external probes (`fio`, `iperf3`, `ping`, `tc netem`) and the benchmark suite, then calls the CLI and `bencher run`. A `perf` job in `release.yml` invokes the orchestrator on the self-hosted runner. The runner image, ARC, node config, and Bencher account are out of scope (separate infra repo).

**Tech Stack:** Go (stdlib + testify suites), `go test -bench`, `fio`, `iperf3`, `iproute2`/`tc`, Bencher CLI, GitHub Actions, Task (go-task).

**Spec:** `docs/superpowers/specs/2026-05-24-perf-tracking-bencher-design.md`

**Environment note:** The `bmf` package tests are pure Go (no FUSE) and run anywhere, including this sandbox and normal CI. Anything that runs the benchmark suite or FUSE mounts must run on the self-hosted runner (or the kubevirt VM) — those steps are marked **[runner-only]**. See memory: FUSE-mount tests fail in the sandbox.

**Conventions:** Tests are methods on a testify suite (not standalone `func TestX`). Commits use conventional-commit subject + short body, no trailers. Module path is `gmountie`.

---

## File structure

**New (in-repo):**
- `test/e2e/perf/bmf/gobench.go` — parse `go test -bench -benchmem` lines.
- `test/e2e/perf/bmf/substrate.go` — `Substrate` types + raw-probe parsers (fio/iperf3/ping JSON+text).
- `test/e2e/perf/bmf/report.go` — `BuildReport`: BMF shaping, normalization, `_substrate/*` series.
- `test/e2e/perf/bmf/bmf_test.go` — testify suite covering all of the above.
- `test/e2e/perf/cmd/perfbmf/main.go` — CLI: `cpuprobe`, `substrate`, `emit`.
- `test/e2e/perf/substrate/substrate.fio` — fio job file (seq + rand, all jobs in one run).
- `scripts/perf/profile.sh` — apply/clear a named netem profile on an interface.
- `scripts/perf/run.sh` — orchestrator: probes → benches → BMF → `bencher run`.

**Modified:**
- `Taskfile.yaml` — add `perf:bmf:build`, `perf:substrate`, `perf:ci` targets.
- `scripts/start-slow-loopback.sh` — delegate to `scripts/perf/profile.sh` so the WAN spec lives in one place.
- `.github/workflows/release.yml` — add the `perf` job.
- `docs/perf/README.md` — document the Bencher pipeline + drift runbook.
- `docs/superpowers/specs/2026-05-24-perf-tracking-bencher-design.md` — flip Status to implemented when done.

---

## Task 1: go-bench output parser

**Files:**
- Create: `test/e2e/perf/bmf/gobench.go`
- Test: `test/e2e/perf/bmf/bmf_test.go`

- [ ] **Step 1: Write the failing test**

Create `test/e2e/perf/bmf/bmf_test.go`:

```go
package bmf

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type BMFSuite struct {
	suite.Suite
}

func TestBMFSuite(t *testing.T) {
	suite.Run(t, new(BMFSuite))
}

const sampleBench = `goos: linux
goarch: amd64
pkg: gmountie/test/e2e/perf
BenchmarkSeqRead64MiB-8   	      18	  64864928 ns/op	1034.99 MB/s	    1234 B/op	      10 allocs/op
BenchmarkSeqRead64MiB-8   	      17	  65000000 ns/op	1032.00 MB/s	    1240 B/op	      11 allocs/op
BenchmarkOpenStatClose-8  	   50000	     30000 ns/op	     128 B/op	       2 allocs/op
PASS
ok  	gmountie/test/e2e/perf	12.345s
`

func (s *BMFSuite) TestParseGoBench() {
	res, err := ParseGoBench(strings.NewReader(sampleBench))
	s.Require().NoError(err)
	s.Require().Len(res, 3)

	// Name has the Benchmark prefix and -GOMAXPROCS suffix stripped.
	s.Equal("SeqRead64MiB", res[0].Name)
	s.InDelta(64864928, res[0].NsPerOp, 0.5)
	s.InDelta(1034.99, res[0].MBPerSec, 0.001)
	s.InDelta(1234, res[0].BytesPerOp, 0.5)
	s.InDelta(10, res[0].AllocsPerOp, 0.5)

	// Metadata benchmark: no MB/s field -> MBPerSec stays 0.
	s.Equal("OpenStatClose", res[2].Name)
	s.Zero(res[2].MBPerSec)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./test/e2e/perf/bmf/ -run TestBMFSuite -v`
Expected: FAIL — `undefined: ParseGoBench`.

- [ ] **Step 3: Write minimal implementation**

Create `test/e2e/perf/bmf/gobench.go`:

```go
// Package bmf converts gMountie perf benchmark output plus a substrate
// fingerprint into Bencher Metric Format (BMF) JSON.
package bmf

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// GoBenchResult is one parsed line of `go test -bench -benchmem` output.
type GoBenchResult struct {
	Name        string  // benchmark name, "Benchmark" prefix and -GOMAXPROCS suffix stripped
	NsPerOp     float64
	MBPerSec    float64 // 0 if the benchmark did not call b.SetBytes
	BytesPerOp  float64
	AllocsPerOp float64
}

// ParseGoBench parses benchmark result lines. Non-benchmark lines are ignored.
// With -count=N there are N lines per benchmark; all are returned in order.
func ParseGoBench(r io.Reader) ([]GoBenchResult, error) {
	var out []GoBenchResult
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 || !strings.HasPrefix(fields[0], "Benchmark") {
			continue
		}
		res := GoBenchResult{Name: trimName(fields[0])}
		// A unit token's numeric value is the immediately preceding field.
		for i := 1; i < len(fields); i++ {
			switch fields[i] {
			case "ns/op":
				res.NsPerOp = atof(fields[i-1])
			case "MB/s":
				res.MBPerSec = atof(fields[i-1])
			case "B/op":
				res.BytesPerOp = atof(fields[i-1])
			case "allocs/op":
				res.AllocsPerOp = atof(fields[i-1])
			}
		}
		out = append(out, res)
	}
	return out, sc.Err()
}

func trimName(field string) string {
	name := strings.TrimPrefix(field, "Benchmark")
	if i := strings.LastIndexByte(name, '-'); i >= 0 {
		if _, err := strconv.Atoi(name[i+1:]); err == nil {
			return name[:i]
		}
	}
	return name
}

func atof(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./test/e2e/perf/bmf/ -run TestBMFSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add test/e2e/perf/bmf/gobench.go test/e2e/perf/bmf/bmf_test.go
git commit -m "feat(perf/bmf): parse go test -bench output

First piece of the BMF emitter: tolerant parser for go test -bench
-benchmem lines, stripping the Benchmark prefix and -GOMAXPROCS suffix
and tolerating the optional MB/s column."
```

---

## Task 2: Substrate types and raw-probe parsers

**Files:**
- Create: `test/e2e/perf/bmf/substrate.go`
- Modify: `test/e2e/perf/bmf/bmf_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `test/e2e/perf/bmf/bmf_test.go`:

```go
const sampleFio = `{
  "jobs": [
    {"jobname": "seqread",  "read":  {"bw_bytes": 524288000, "iops": 500.0}, "write": {"bw_bytes": 0, "iops": 0}},
    {"jobname": "seqwrite", "read":  {"bw_bytes": 0, "iops": 0}, "write": {"bw_bytes": 471859200, "iops": 450.0}},
    {"jobname": "randread", "read":  {"bw_bytes": 49152000, "iops": 12000.0}, "write": {"bw_bytes": 0, "iops": 0}},
    {"jobname": "randwrite","read":  {"bw_bytes": 0, "iops": 0}, "write": {"bw_bytes": 36864000, "iops": 9000.0}}
  ]
}`

const sampleIperf = `{"end": {"sum_received": {"bits_per_second": 100000000.0}}}`

const samplePing = `PING 127.0.0.1 (127.0.0.1) 56(84) bytes of data.
--- 127.0.0.1 ping statistics ---
20 packets transmitted, 20 received, 0% packet loss, time 19000ms
rtt min/avg/max/mdev = 49.001/50.250/51.500/0.700 ms
`

func (s *BMFSuite) TestParseFio() {
	jobs, err := ParseFio(strings.NewReader(sampleFio))
	s.Require().NoError(err)
	// bw_bytes / 1e6 == MB/s, matching Go's MB/s convention.
	s.InDelta(524.288, jobs["seqread"].Read.BwMBs, 0.001)
	s.InDelta(471.8592, jobs["seqwrite"].Write.BwMBs, 0.001)
	s.InDelta(12000, jobs["randread"].Read.IOPS, 0.5)
	s.InDelta(9000, jobs["randwrite"].Write.IOPS, 0.5)
}

func (s *BMFSuite) TestParseIperf3Mbs() {
	mbs, err := ParseIperf3MBs(strings.NewReader(sampleIperf))
	s.Require().NoError(err)
	// 100 Mbit/s == 12.5 MB/s.
	s.InDelta(12.5, mbs, 0.001)
}

func (s *BMFSuite) TestParsePingAvgRTT() {
	rtt, err := ParsePingAvgRTT(strings.NewReader(samplePing))
	s.Require().NoError(err)
	s.InDelta(50.250, rtt, 0.001)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./test/e2e/perf/bmf/ -run TestBMFSuite -v`
Expected: FAIL — `undefined: ParseFio` (and friends).

- [ ] **Step 3: Write the implementation**

Create `test/e2e/perf/bmf/substrate.go`:

```go
package bmf

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
)

// Substrate is the raw machine fingerprint captured before gMountie runs.
// It is used both to normalize results and to surface floor drift as its
// own dashboard series.
type Substrate struct {
	CPUOpsPerSec float64             `json:"cpu_ops_per_sec"`
	Disk         DiskSubstrate       `json:"disk"`
	Net          map[string]NetProbe `json:"net"` // keyed by profile: "lan", "wan"
}

type DiskSubstrate struct {
	SeqReadMBs      float64 `json:"seq_read_mbs"`
	SeqWriteMBs     float64 `json:"seq_write_mbs"`
	Rand4kReadIOPS  float64 `json:"rand_4k_read_iops"`
	Rand4kWriteIOPS float64 `json:"rand_4k_write_iops"`
}

type NetProbe struct {
	RTTms        float64 `json:"rtt_ms"`
	BandwidthMBs float64 `json:"bandwidth_mbs"`
}

// FioSide holds the read- or write-side numbers of one fio job, in MB/s and IOPS.
type FioSide struct {
	BwMBs float64
	IOPS  float64
}

// FioJob bundles both sides of a single fio job.
type FioJob struct {
	Read  FioSide
	Write FioSide
}

// ParseFio parses `fio --output-format=json` and returns jobs keyed by jobname.
func ParseFio(r io.Reader) (map[string]FioJob, error) {
	var raw struct {
		Jobs []struct {
			JobName string `json:"jobname"`
			Read    struct {
				BwBytes float64 `json:"bw_bytes"`
				IOPS    float64 `json:"iops"`
			} `json:"read"`
			Write struct {
				BwBytes float64 `json:"bw_bytes"`
				IOPS    float64 `json:"iops"`
			} `json:"write"`
		} `json:"jobs"`
	}
	if err := json.NewDecoder(r).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode fio json: %w", err)
	}
	out := make(map[string]FioJob, len(raw.Jobs))
	for _, j := range raw.Jobs {
		out[j.JobName] = FioJob{
			Read:  FioSide{BwMBs: j.Read.BwBytes / 1e6, IOPS: j.Read.IOPS},
			Write: FioSide{BwMBs: j.Write.BwBytes / 1e6, IOPS: j.Write.IOPS},
		}
	}
	return out, nil
}

// ParseIperf3MBs parses `iperf3 -J` and returns the received bandwidth in MB/s.
func ParseIperf3MBs(r io.Reader) (float64, error) {
	var raw struct {
		End struct {
			SumReceived struct {
				BitsPerSecond float64 `json:"bits_per_second"`
			} `json:"sum_received"`
		} `json:"end"`
	}
	if err := json.NewDecoder(r).Decode(&raw); err != nil {
		return 0, fmt.Errorf("decode iperf3 json: %w", err)
	}
	return raw.End.SumReceived.BitsPerSecond / 8 / 1e6, nil
}

var pingRTT = regexp.MustCompile(`rtt [^=]*= [0-9.]+/([0-9.]+)/`)

// ParsePingAvgRTT extracts the average RTT (ms) from `ping` summary output.
func ParsePingAvgRTT(r io.Reader) (float64, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	m := pingRTT.FindSubmatch(b)
	if m == nil {
		return 0, fmt.Errorf("no rtt summary line in ping output")
	}
	return atof(string(m[1])), nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./test/e2e/perf/bmf/ -run TestBMFSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add test/e2e/perf/bmf/substrate.go test/e2e/perf/bmf/bmf_test.go
git commit -m "feat(perf/bmf): substrate types and probe parsers

Add Substrate/DiskSubstrate/NetProbe types and tested parsers for fio
JSON (bw_bytes->MB/s, IOPS by jobname), iperf3 JSON (bits/s->MB/s) and
ping average RTT."
```

---

## Task 3: BuildReport — BMF shaping, normalization, substrate series

**Files:**
- Create: `test/e2e/perf/bmf/report.go`
- Modify: `test/e2e/perf/bmf/bmf_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `test/e2e/perf/bmf/bmf_test.go`:

```go
func (s *BMFSuite) sampleSubstrate() Substrate {
	return Substrate{
		CPUOpsPerSec: 1.5e8,
		Disk:         DiskSubstrate{SeqReadMBs: 2000, SeqWriteMBs: 1800, Rand4kReadIOPS: 12000, Rand4kWriteIOPS: 9000},
		Net: map[string]NetProbe{
			"lan": {RTTms: 0.05, BandwidthMBs: 5000},
			"wan": {RTTms: 50.0, BandwidthMBs: 12.5},
		},
	}
}

func (s *BMFSuite) TestBuildReportNormalizesSeqAgainstBindingCeiling() {
	// SeqRead at 1000 MB/s. LAN ceiling = min(disk 2000, net 5000) = 2000 -> 50%.
	// WAN ceiling = min(disk 2000, net 12.5) = 12.5; SeqRead WAN at 12 MB/s -> 96%.
	results := map[string][]GoBenchResult{
		"lan": {{Name: "SeqRead64MiB", NsPerOp: 1e6, MBPerSec: 1000}},
		"wan": {{Name: "SeqRead64MiB", NsPerOp: 5e6, MBPerSec: 12}},
	}
	rep := BuildReport(results, s.sampleSubstrate())

	s.InDelta(50.0, rep["SeqRead64MiB/lan"]["throughput_pct_of_raw"].Value, 0.01)
	s.InDelta(96.0, rep["SeqRead64MiB/wan"]["throughput_pct_of_raw"].Value, 0.01)
	s.InDelta(1000, rep["SeqRead64MiB/lan"]["throughput"].Value, 0.01)
	s.InDelta(1e6, rep["SeqRead64MiB/lan"]["latency"].Value, 0.5)
}

func (s *BMFSuite) TestBuildReportMetadataHasNoThroughput() {
	results := map[string][]GoBenchResult{
		"lan": {{Name: "OpenStatClose", NsPerOp: 30000}},
	}
	rep := BuildReport(results, s.sampleSubstrate())
	m := rep["OpenStatClose/lan"]
	s.Contains(m, "latency")
	s.NotContains(m, "throughput")
	s.NotContains(m, "throughput_pct_of_raw")
}

func (s *BMFSuite) TestBuildReportRandomHasThroughputButNoNormalization() {
	results := map[string][]GoBenchResult{
		"lan": {{Name: "RandomRead4KiB", NsPerOp: 5000, MBPerSec: 0.8}},
	}
	rep := BuildReport(results, s.sampleSubstrate())
	m := rep["RandomRead4KiB/lan"]
	s.Contains(m, "throughput")
	s.NotContains(m, "throughput_pct_of_raw") // no principled seq ceiling for random
}

func (s *BMFSuite) TestBuildReportAggregatesBounds() {
	results := map[string][]GoBenchResult{
		"lan": {
			{Name: "SeqWrite1MiB", NsPerOp: 100, MBPerSec: 10},
			{Name: "SeqWrite1MiB", NsPerOp: 200, MBPerSec: 20},
		},
	}
	rep := BuildReport(results, s.sampleSubstrate())
	lat := rep["SeqWrite1MiB/lan"]["latency"]
	s.InDelta(150, lat.Value, 0.01)            // mean
	s.Require().NotNil(lat.LowerValue)
	s.Require().NotNil(lat.UpperValue)
	s.InDelta(100, *lat.LowerValue, 0.01)      // min
	s.InDelta(200, *lat.UpperValue, 0.01)      // max
}

func (s *BMFSuite) TestBuildReportEmitsSubstrateSeries() {
	rep := BuildReport(map[string][]GoBenchResult{}, s.sampleSubstrate())
	s.InDelta(1.5e8, rep["_substrate/cpu_compute"]["ops_per_sec"].Value, 1)
	s.InDelta(2000, rep["_substrate/disk_seq_read"]["throughput"].Value, 0.01)
	s.InDelta(1800, rep["_substrate/disk_seq_write"]["throughput"].Value, 0.01)
	s.InDelta(50.0, rep["_substrate/net_rtt_wan"]["latency"].Value, 0.01)
	s.InDelta(0.05, rep["_substrate/net_rtt_lan"]["latency"].Value, 0.01)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./test/e2e/perf/bmf/ -run TestBMFSuite -v`
Expected: FAIL — `undefined: BuildReport`.

- [ ] **Step 3: Write the implementation**

Create `test/e2e/perf/bmf/report.go`:

```go
package bmf

import (
	"math"
	"strings"
)

// Metric is a BMF metric: a central value with optional bounds.
type Metric struct {
	Value      float64  `json:"value"`
	LowerValue *float64 `json:"lower_value,omitempty"`
	UpperValue *float64 `json:"upper_value,omitempty"`
}

// Report is a BMF document: benchmark name -> measure name -> metric.
type Report map[string]map[string]Metric

// BuildReport turns per-profile benchmark results plus a substrate fingerprint
// into a BMF document. results is keyed by profile ("lan"/"wan").
func BuildReport(results map[string][]GoBenchResult, sub Substrate) Report {
	rep := Report{}
	for profile, list := range results {
		net := sub.Net[profile]
		for name, runs := range groupByName(list) {
			bench := name + "/" + profile
			m := map[string]Metric{
				"latency": aggregate(runs, func(r GoBenchResult) float64 { return r.NsPerOp }),
			}
			if anyThroughput(runs) {
				m["throughput"] = aggregate(runs, func(r GoBenchResult) float64 { return r.MBPerSec })
				if ceil := seqCeiling(name, sub.Disk, net); ceil > 0 {
					m["throughput_pct_of_raw"] = aggregate(runs, func(r GoBenchResult) float64 {
						return r.MBPerSec / ceil * 100
					})
				}
			}
			rep[bench] = m
		}
	}
	addSubstrate(rep, sub)
	return rep
}

func groupByName(list []GoBenchResult) map[string][]GoBenchResult {
	out := map[string][]GoBenchResult{}
	for _, r := range list {
		out[r.Name] = append(out[r.Name], r)
	}
	return out
}

func anyThroughput(runs []GoBenchResult) bool {
	for _, r := range runs {
		if r.MBPerSec > 0 {
			return true
		}
	}
	return false
}

// seqCeiling returns the binding throughput ceiling (MB/s) for sequential
// benchmarks: the slower of the local disk and the (possibly shaped) link.
// Returns 0 for non-sequential benchmarks, which suppresses normalization.
func seqCeiling(name string, disk DiskSubstrate, net NetProbe) float64 {
	switch {
	case strings.HasPrefix(name, "SeqRead"):
		return minPos(disk.SeqReadMBs, net.BandwidthMBs)
	case strings.HasPrefix(name, "SeqWrite"):
		return minPos(disk.SeqWriteMBs, net.BandwidthMBs)
	default:
		return 0
	}
}

func minPos(a, b float64) float64 {
	switch {
	case a <= 0:
		return b
	case b <= 0:
		return a
	default:
		return math.Min(a, b)
	}
}

func aggregate(runs []GoBenchResult, f func(GoBenchResult) float64) Metric {
	if len(runs) == 0 {
		return Metric{}
	}
	lo, hi, sum := math.Inf(1), math.Inf(-1), 0.0
	for _, r := range runs {
		v := f(r)
		sum += v
		lo, hi = math.Min(lo, v), math.Max(hi, v)
	}
	mean := sum / float64(len(runs))
	loC, hiC := lo, hi
	return Metric{Value: mean, LowerValue: &loC, UpperValue: &hiC}
}

// addSubstrate appends the raw fingerprint as its own _substrate/* benchmarks
// so floor drift is visible on the dashboard, not only folded into ratios.
// Measure-name convention (stable — renaming breaks Bencher series continuity):
// MB/s -> "throughput", time -> "latency", rates -> "iops"/"ops_per_sec".
func addSubstrate(rep Report, sub Substrate) {
	rep["_substrate/cpu_compute"] = map[string]Metric{"ops_per_sec": {Value: sub.CPUOpsPerSec}}
	rep["_substrate/disk_seq_read"] = map[string]Metric{"throughput": {Value: sub.Disk.SeqReadMBs}}
	rep["_substrate/disk_seq_write"] = map[string]Metric{"throughput": {Value: sub.Disk.SeqWriteMBs}}
	rep["_substrate/disk_rand_4k_read_iops"] = map[string]Metric{"iops": {Value: sub.Disk.Rand4kReadIOPS}}
	rep["_substrate/disk_rand_4k_write_iops"] = map[string]Metric{"iops": {Value: sub.Disk.Rand4kWriteIOPS}}
	for profile, n := range sub.Net {
		rep["_substrate/net_rtt_"+profile] = map[string]Metric{"latency": {Value: n.RTTms}}
		rep["_substrate/net_bw_"+profile] = map[string]Metric{"throughput": {Value: n.BandwidthMBs}}
	}
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./test/e2e/perf/bmf/ -run TestBMFSuite -v`
Expected: PASS (all suite methods).

- [ ] **Step 5: Run vet + lint for the package**

Run: `go vet ./test/e2e/perf/bmf/ && go run github.com/golangci/golangci-lint/cmd/golangci-lint run ./test/e2e/perf/bmf/...`
Expected: no findings. (If golangci-lint flags the unused `_ = profile` style, fix inline.)

- [ ] **Step 6: Commit**

```bash
git add test/e2e/perf/bmf/report.go test/e2e/perf/bmf/bmf_test.go
git commit -m "feat(perf/bmf): build BMF report with normalization

BuildReport groups runs per benchmark, emits latency + throughput +
throughput_pct_of_raw (sequential only, against min(disk,link) ceiling),
aggregates count>1 runs into mean/min/max, and appends the raw substrate
fingerprint as _substrate/* series for drift visibility."
```

---

## Task 4: perfbmf CLI (cpuprobe, substrate, emit)

**Files:**
- Create: `test/e2e/perf/cmd/perfbmf/main.go`

This task is CLI wiring around the tested library; the logic is already covered. Validate by building and running against the sample fixtures.

- [ ] **Step 1: Write the implementation**

Create `test/e2e/perf/cmd/perfbmf/main.go`:

```go
// Command perfbmf is the CLI front-end for the bmf library. Subcommands:
//
//	perfbmf cpuprobe
//	    Run a fixed-duration compute loop; print ops/sec as a bare number.
//	perfbmf substrate --cpu N --fio f.json \
//	    --iperf-lan f.json --ping-lan f.txt \
//	    --iperf-wan f.json --ping-wan f.txt [--out substrate.json]
//	    Assemble a substrate.json from raw probe outputs.
//	perfbmf emit --substrate substrate.json \
//	    --bench-lan lan.txt --bench-wan wan.txt [--out report.bmf.json]
//	    Produce the BMF document.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"gmountie/test/e2e/perf/bmf"
)

func main() {
	if len(os.Args) < 2 {
		fail("usage: perfbmf <cpuprobe|substrate|emit> ...")
	}
	switch os.Args[1] {
	case "cpuprobe":
		cpuprobe()
	case "substrate":
		substrate(os.Args[2:])
	case "emit":
		emit(os.Args[2:])
	default:
		fail("unknown subcommand %q", os.Args[1])
	}
}

// cpuprobe runs a deterministic integer workload for ~2s and prints ops/sec.
// Crude on purpose: it only needs to surface gross CPU-floor drift, and a
// loose Bencher threshold absorbs the noise.
func cpuprobe() {
	const window = 2 * time.Second
	deadline := time.Now().Add(window)
	var ops uint64
	var acc uint64 = 1
	for time.Now().Before(deadline) {
		for i := 0; i < 1_000_000; i++ {
			acc = acc*6364136223846793005 + 1442695040888963407 // PCG-ish LCG step
		}
		ops += 1_000_000
	}
	_ = acc
	fmt.Printf("%g\n", float64(ops)/window.Seconds())
}

func substrate(args []string) {
	fs := flag.NewFlagSet("substrate", flag.ExitOnError)
	cpu := fs.Float64("cpu", 0, "cpu ops/sec from `perfbmf cpuprobe`")
	fioPath := fs.String("fio", "", "fio --output-format=json file")
	iperfLAN := fs.String("iperf-lan", "", "iperf3 -J file for the lan profile")
	pingLAN := fs.String("ping-lan", "", "ping output file for the lan profile")
	iperfWAN := fs.String("iperf-wan", "", "iperf3 -J file for the wan profile")
	pingWAN := fs.String("ping-wan", "", "ping output file for the wan profile")
	out := fs.String("out", "", "output path (default stdout)")
	_ = fs.Parse(args)

	jobs := must(bmf.ParseFio(mustOpen(*fioPath)))
	sub := bmf.Substrate{
		CPUOpsPerSec: *cpu,
		Disk: bmf.DiskSubstrate{
			SeqReadMBs:      jobs["seqread"].Read.BwMBs,
			SeqWriteMBs:     jobs["seqwrite"].Write.BwMBs,
			Rand4kReadIOPS:  jobs["randread"].Read.IOPS,
			Rand4kWriteIOPS: jobs["randwrite"].Write.IOPS,
		},
		Net: map[string]bmf.NetProbe{
			"lan": {RTTms: must(bmf.ParsePingAvgRTT(mustOpen(*pingLAN))), BandwidthMBs: must(bmf.ParseIperf3MBs(mustOpen(*iperfLAN)))},
			"wan": {RTTms: must(bmf.ParsePingAvgRTT(mustOpen(*pingWAN))), BandwidthMBs: must(bmf.ParseIperf3MBs(mustOpen(*iperfWAN)))},
		},
	}
	writeJSON(*out, sub)
}

func emit(args []string) {
	fs := flag.NewFlagSet("emit", flag.ExitOnError)
	subPath := fs.String("substrate", "", "substrate.json")
	lan := fs.String("bench-lan", "", "go test -bench output for the lan profile")
	wan := fs.String("bench-wan", "", "go test -bench output for the wan profile")
	out := fs.String("out", "", "output path (default stdout)")
	_ = fs.Parse(args)

	var sub bmf.Substrate
	if err := json.NewDecoder(mustOpen(*subPath)).Decode(&sub); err != nil {
		fail("decode substrate: %v", err)
	}
	results := map[string][]bmf.GoBenchResult{
		"lan": must(bmf.ParseGoBench(mustOpen(*lan))),
		"wan": must(bmf.ParseGoBench(mustOpen(*wan))),
	}
	writeJSON(*out, bmf.BuildReport(results, sub))
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fail("marshal: %v", err)
	}
	if path == "" {
		fmt.Println(string(b))
		return
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		fail("write %s: %v", path, err)
	}
}

func mustOpen(path string) *os.File {
	f, err := os.Open(path)
	if err != nil {
		fail("open %s: %v", path, err)
	}
	return f
}

func must[T any](v T, err error) T {
	if err != nil {
		fail("%v", err)
	}
	return v
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "perfbmf: "+format+"\n", a...)
	os.Exit(1)
}
```

- [ ] **Step 2: Build it**

Run: `go build -o /tmp/perfbmf ./test/e2e/perf/cmd/perfbmf/`
Expected: builds clean.

- [ ] **Step 3: Smoke-test `emit` against fixtures**

```bash
cat > /tmp/sub.json <<'JSON'
{"cpu_ops_per_sec":1.5e8,"disk":{"seq_read_mbs":2000,"seq_write_mbs":1800,"rand_4k_read_iops":12000,"rand_4k_write_iops":9000},"net":{"lan":{"rtt_ms":0.05,"bandwidth_mbs":5000},"wan":{"rtt_ms":50,"bandwidth_mbs":12.5}}}
JSON
printf 'BenchmarkSeqRead64MiB-8\t18\t64864928 ns/op\t1034.99 MB/s\t1234 B/op\t10 allocs/op\n' > /tmp/lan.txt
printf 'BenchmarkSeqRead64MiB-8\t5\t250000000 ns/op\t12.00 MB/s\t1234 B/op\t10 allocs/op\n' > /tmp/wan.txt
/tmp/perfbmf emit --substrate /tmp/sub.json --bench-lan /tmp/lan.txt --bench-wan /tmp/wan.txt
```
Expected: BMF JSON with `SeqRead64MiB/lan`, `SeqRead64MiB/wan` (the wan `throughput_pct_of_raw` ≈ 96), and `_substrate/*` entries.

- [ ] **Step 4: Smoke-test `cpuprobe`**

Run: `/tmp/perfbmf cpuprobe`
Expected: a single positive number (ops/sec).

- [ ] **Step 5: Commit**

```bash
git add test/e2e/perf/cmd/perfbmf/main.go
git commit -m "feat(perf/bmf): perfbmf CLI (cpuprobe, substrate, emit)

Thin CLI over the bmf library: cpuprobe prints a CPU ops/sec floor,
substrate assembles substrate.json from raw fio/iperf3/ping outputs, emit
produces the BMF document for bencher run --adapter json --file."
```

---

## Task 5: fio job file + netem profile script

**Files:**
- Create: `test/e2e/perf/substrate/substrate.fio`
- Create: `scripts/perf/profile.sh`
- Modify: `scripts/start-slow-loopback.sh`

- [ ] **Step 1: Create the fio job file**

Create `test/e2e/perf/substrate/substrate.fio`:

```ini
; Substrate disk probe: one fio run, four named jobs. Targets SUBSTRATE_DIR
; (set to the same filesystem the gMountie volume backing dir uses, so the
; numbers reflect the disk the benchmarks actually hit). direct=1 bypasses the
; page cache to measure the device floor, not RAM.
[global]
directory=${SUBSTRATE_DIR}
ioengine=psync
direct=1
size=256m
time_based=0
group_reporting=1
stonewall=1

[seqread]
rw=read
bs=1m

[seqwrite]
rw=write
bs=1m

[randread]
rw=randread
bs=4k

[randwrite]
rw=randwrite
bs=4k
```

- [ ] **Step 2: Create the profile script**

Create `scripts/perf/profile.sh`:

```bash
#!/usr/bin/env bash
# Apply or clear a named perf network profile via tc netem.
#
# Usage:
#   profile.sh apply <lan|wan> [iface]   # default iface: lo
#   profile.sh clear [iface]
#
# Profiles (the single source of truth for LAN/WAN shaping):
#   lan  -> no shaping (qdisc cleared)
#   wan  -> netem delay 25ms 5ms rate 100Mbit
#
# Note: on loopback a packet traverses the qdisc once per direction, so a
# 25ms delay yields ~50ms RTT. The run harness asserts against that doubled
# value. Requires root (privileged Pod / sudo).
set -euo pipefail

WAN_NETEM="delay 25ms 5ms rate 100Mbit"

cmd="${1:-}"
case "$cmd" in
  apply)
    profile="${2:-}"
    iface="${3:-lo}"
    case "$profile" in
      lan) tc qdisc del dev "$iface" root 2>/dev/null || true ;;
      wan)
        # shellcheck disable=SC2086 # WAN_NETEM is an intentional arg list
        tc qdisc replace dev "$iface" root netem $WAN_NETEM 2>/dev/null \
          || tc qdisc add dev "$iface" root netem $WAN_NETEM ;;
      *) echo "profile.sh: unknown profile '$profile' (want lan|wan)" >&2; exit 2 ;;
    esac
    tc qdisc show dev "$iface" ;;
  clear)
    iface="${2:-lo}"
    tc qdisc del dev "$iface" root 2>/dev/null || true
    echo "cleared netem on $iface" ;;
  *)
    echo "usage: profile.sh apply <lan|wan> [iface] | clear [iface]" >&2
    exit 2 ;;
esac
```

- [ ] **Step 3: Point start-slow-loopback at the shared WAN spec**

Replace the body of `scripts/start-slow-loopback.sh` (keep the shebang + header comment) so the WAN profile is defined in one place. New body after the header:

```bash
set -euo pipefail
# Back-compat wrapper: the canonical profile definitions now live in
# scripts/perf/profile.sh. Default behaviour applies the WAN profile to lo.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$here/perf/profile.sh" apply "${1:-wan}" "${2:-lo}"
```

- [ ] **Step 4: Make scripts executable + shellcheck**

```bash
chmod +x scripts/perf/profile.sh
shellcheck scripts/perf/profile.sh scripts/start-slow-loopback.sh
```
Expected: shellcheck clean (the SC2086 on `$WAN_NETEM` is suppressed inline). If `shellcheck` isn't installed locally, note it and rely on the runner.

- [ ] **Step 5: Commit**

```bash
git add test/e2e/perf/substrate/substrate.fio scripts/perf/profile.sh scripts/start-slow-loopback.sh
git commit -m "feat(perf): fio substrate job + named netem profiles

Single fio job file (seq+rand, named jobs) for the disk fingerprint, and
scripts/perf/profile.sh as the one source of truth for the lan/wan netem
profiles. start-slow-loopback.sh now delegates to it."
```

---

## Task 6: run.sh orchestrator

**Files:**
- Create: `scripts/perf/run.sh`

This wires the whole sequence. It is **[runner-only]** to execute end-to-end (needs FUSE + tc), but is written so the dry-run validation step (Task 8) exercises it.

- [ ] **Step 1: Write the orchestrator**

Create `scripts/perf/run.sh`:

```bash
#!/usr/bin/env bash
# Orchestrate one perf run: substrate probe -> benches over lan+wan ->
# BMF -> (optional) bencher upload. Designed to run on the self-hosted
# perf runner. Honours these env vars:
#
#   COUNT        go test -bench -count        (default 10)
#   BENCHTIME    go test -benchtime           (default 10s)
#   WORKDIR      scratch dir; MUST be on the local PV (default ./perf-out)
#   SUBSTRATE_DIR  fio probe dir (default $TMPDIR == bench data dir)
#   IFACE        interface to shape           (default lo)
#   EXPECT_GO_VERSION / EXPECT_FIO_VERSION / EXPECT_IPERF3_VERSION /
#   EXPECT_BENCHER_VERSION  optional pinned versions; mismatch fails the run
#   BENCHER      "1" to run bencher upload    (default unset = skip)
#   BENCHER_PROJECT / BENCHER_TESTBED / BENCHER_BRANCH / GIT_HASH
#
# Requires: go, fio, iperf3, ping, tc, and a built perfbmf on PATH or at
# $PERFBMF (default: built into $WORKDIR).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

COUNT="${COUNT:-10}"
BENCHTIME="${BENCHTIME:-10s}"
WORKDIR="${WORKDIR:-$repo/perf-out}"
# The bench harness creates its data dir via os.MkdirTemp("", ...), which
# honours $TMPDIR. Point TMPDIR and the fio probe at the SAME directory so the
# substrate disk number reflects the filesystem the benches actually hit.
# WORKDIR must live on the runner's local PV — not the ephemeral overlay, and
# not tmpfs (fio direct=1 needs a real block-backed fs).
export TMPDIR="$WORKDIR/data"
export SUBSTRATE_DIR="${SUBSTRATE_DIR:-$TMPDIR}"
IFACE="${IFACE:-lo}"
PERFBMF="${PERFBMF:-$WORKDIR/perfbmf}"

mkdir -p "$WORKDIR" "$TMPDIR" "$SUBSTRATE_DIR"

# Always leave the interface unshaped on exit, even on failure.
cleanup() { "$here/profile.sh" clear "$IFACE" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "== assert toolchain present =="
for bin in go fio iperf3 ping tc; do command -v "$bin" >/dev/null || { echo "missing $bin" >&2; exit 1; }; done
[ "${BENCHER:-}" = "1" ] && { command -v bencher >/dev/null || { echo "missing bencher" >&2; exit 1; }; }

echo "== assert pinned tool versions =="
# Pins live with the runner image (infra repo) and arrive via EXPECT_* env.
# When set, a drifted tool fails the run loudly instead of silently moving
# the substrate floor — the spec's runner-upkeep mitigation, enforced here.
assert_version() { # $1=label $2=actual $3=expected(optional)
  echo "$1: $2"
  if [ -n "${3:-}" ] && [ "$2" != "$3" ]; then echo "  ERROR: expected '$3'" >&2; exit 1; fi
}
assert_version go      "$(go version | awk '{print $3}')"                "${EXPECT_GO_VERSION:-}"
assert_version fio     "$(fio --version)"                                "${EXPECT_FIO_VERSION:-}"
assert_version iperf3  "$(iperf3 --version | head -1 | awk '{print $2}')" "${EXPECT_IPERF3_VERSION:-}"
[ "${BENCHER:-}" = "1" ] && assert_version bencher "$(bencher --version | awk '{print $NF}')" "${EXPECT_BENCHER_VERSION:-}"

echo "== build perfbmf =="
go build -o "$PERFBMF" ./test/e2e/perf/cmd/perfbmf/

echo "== cpu + disk substrate =="
cpu="$("$PERFBMF" cpuprobe)"
fio --output-format=json "$repo/test/e2e/perf/substrate/substrate.fio" > "$WORKDIR/fio.json"

run_net_probe() { # $1=profile $2=iperf seconds
  local p="$1" secs="$2"
  "$here/profile.sh" apply "$p" "$IFACE" >/dev/null
  iperf3 -s -1 -D                                  # one-shot server, daemonized
  sleep 0.3
  # WAN needs a longer window so TCP slow-start doesn't drag the average
  # below the 100 Mbit cap; LAN over lo settles almost instantly.
  iperf3 -c 127.0.0.1 -t "$secs" -J > "$WORKDIR/iperf-$p.json"
  ping -c 20 -i 0.1 127.0.0.1 > "$WORKDIR/ping-$p.txt"
}

run_bench() { # $1=profile
  local p="$1"
  GMOUNTIE_BENCH_TCP=1 go test -run=^$ -bench=. -benchmem \
    -count="$COUNT" -benchtime="$BENCHTIME" ./test/e2e/perf/ \
    | tee "$WORKDIR/bench-$p.txt"
}

assert_rtt() { # $1=profile $2=min $3=max
  local rtt; rtt="$(awk -F/ '/rtt/{print $5}' "$WORKDIR/ping-$1.txt")"
  awk -v r="$rtt" -v lo="$2" -v hi="$3" 'BEGIN{exit !(r>=lo && r<=hi)}' \
    || { echo "profile $1 RTT ${rtt}ms outside [$2,$3] — netem not applied as expected" >&2; exit 1; }
}

for p in lan wan; do
  echo "== profile $p: net probe =="
  if [ "$p" = wan ]; then run_net_probe "$p" 8; else run_net_probe "$p" 3; fi
  echo "== profile $p: benches =="
  run_bench "$p"
done
"$here/profile.sh" clear "$IFACE" >/dev/null

# LAN loopback RTT is sub-millisecond; WAN delay 25ms -> ~50ms RTT round trip.
assert_rtt lan 0 5
assert_rtt wan 40 60

echo "== assemble substrate.json =="
"$PERFBMF" substrate --cpu "$cpu" --fio "$WORKDIR/fio.json" \
  --iperf-lan "$WORKDIR/iperf-lan.json" --ping-lan "$WORKDIR/ping-lan.txt" \
  --iperf-wan "$WORKDIR/iperf-wan.json" --ping-wan "$WORKDIR/ping-wan.txt" \
  --out "$WORKDIR/substrate.json"

echo "== emit BMF =="
"$PERFBMF" emit --substrate "$WORKDIR/substrate.json" \
  --bench-lan "$WORKDIR/bench-lan.txt" --bench-wan "$WORKDIR/bench-wan.txt" \
  --out "$WORKDIR/report.bmf.json"

if [ "${BENCHER:-}" = "1" ]; then
  echo "== bencher run =="
  bencher run \
    --project "$BENCHER_PROJECT" \
    --branch "${BENCHER_BRANCH:-master}" \
    --testbed "$BENCHER_TESTBED" \
    --hash "${GIT_HASH:-$(git rev-parse HEAD)}" \
    --adapter json \
    --file "$WORKDIR/report.bmf.json"
else
  echo "BENCHER!=1, skipping upload; report at $WORKDIR/report.bmf.json"
fi
```

- [ ] **Step 2: shellcheck**

```bash
chmod +x scripts/perf/run.sh
shellcheck scripts/perf/run.sh
```
Expected: clean (or only intentional/suppressed findings). Fix any real issues inline.

- [ ] **Step 3: Commit**

```bash
git add scripts/perf/run.sh
git commit -m "feat(perf): run.sh orchestrator

End-to-end perf run: cpu+disk substrate, per-profile net probe + bench
suite over loopback TCP with netem, RTT sanity assertion (wan ~50ms on
lo), substrate.json + BMF emission, and an opt-in bencher upload. Always
clears netem on exit."
```

---

## Task 7: Taskfile targets

**Files:**
- Modify: `Taskfile.yaml` (perf section, after `perf:diff` near line 160)

- [ ] **Step 1: Add the targets**

Add under the perf section of `Taskfile.yaml`:

```yaml
  perf:bmf:build:
    desc: Build the perfbmf BMF emitter into ./perf-out/perfbmf.
    cmds:
      - mkdir -p perf-out
      - go build -o perf-out/perfbmf ./test/e2e/perf/cmd/perfbmf/

  perf:ci:
    desc: 'Full perf run (substrate + lan/wan benches + BMF). Set BENCHER=1 to upload. Vars via env: COUNT, BENCHTIME, BENCHER_PROJECT, BENCHER_TESTBED, BENCHER_BRANCH, GIT_HASH. Runner-only (needs FUSE + tc).'
    cmds:
      - bash scripts/perf/run.sh
```

- [ ] **Step 2: Verify Task parses**

Run: `task --list | grep perf`
Expected: lists `perf:bmf:build` and `perf:ci` alongside the existing perf targets.

- [ ] **Step 3: Commit**

```bash
git add Taskfile.yaml
git commit -m "feat(perf): task perf:bmf:build and perf:ci targets

perf:bmf:build compiles the emitter; perf:ci runs the full orchestrator
(runner-only). Both reuse scripts/perf/run.sh so local and CI paths match."
```

---

## Task 8: release.yml perf job

**Files:**
- Modify: `.github/workflows/release.yml`

- [ ] **Step 1: Add the perf job**

Append this job to `.github/workflows/release.yml` (sibling of the existing `release` job):

```yaml
  perf:
    needs: [release]
    if: ${{ github.event.inputs.type != 'snapshot' }}
    runs-on: [self-hosted, gmountie-perf]
    steps:
      - name: Checkout the released tag
        uses: actions/checkout@v6
        with:
          fetch-depth: 0
          fetch-tags: true

      - name: Resolve released tag
        id: tag
        run: echo "ref=$(git describe --tags --abbrev=0)" >> "$GITHUB_OUTPUT"

      - name: Run perf suite and upload to Bencher
        env:
          BENCHER: "1"
          BENCHER_API_TOKEN: ${{ secrets.BENCHER_API_TOKEN }}
          BENCHER_PROJECT: gmountie
          BENCHER_TESTBED: gmountie-perf-pod
          BENCHER_BRANCH: master
          GIT_HASH: ${{ github.sha }}
          COUNT: "10"
          BENCHTIME: "10s"
          # WORKDIR must point at the runner's node-local PV mount (contract
          # with the infra repo) so fio + benches hit a stable local disk, not
          # the ephemeral overlay or tmpfs. Adjust the path to the actual mount.
          WORKDIR: /mnt/perf
        run: bash scripts/perf/run.sh

      - name: Upload raw artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: perf-${{ steps.tag.outputs.ref }}
          path: |
            perf-out/bench-lan.txt
            perf-out/bench-wan.txt
            perf-out/substrate.json
            perf-out/report.bmf.json
          if-no-files-found: warn
```

- [ ] **Step 2: Lint the workflow**

Run: `actionlint .github/workflows/release.yml`
Expected: no errors. (If `actionlint` isn't installed: `go run github.com/rhysd/actionlint/cmd/actionlint@latest .github/workflows/release.yml`.)

- [ ] **Step 3: [runner-only] Dry-run the orchestrator on the runner**

On the self-hosted runner (or kubevirt VM), from a checkout of the released-tag-equivalent:

```bash
BENCHER= COUNT=2 BENCHTIME=2s bash scripts/perf/run.sh
```
Expected: completes; `perf-out/report.bmf.json` exists and validates as BMF (`jq . perf-out/report.bmf.json` parses; contains `SeqRead64MiB/lan`, `SeqRead64MiB/wan`, `_substrate/*`); the wan RTT assertion passes (~50ms). This is the first real end-to-end check — FUSE, tc, fio, iperf3 all exercised.

- [ ] **Step 4: [runner-only] One real upload**

With `BENCHER=1` and a valid `BENCHER_API_TOKEN`, confirm a datapoint lands on the Bencher dashboard under testbed `gmountie-perf-pod`, branch `master`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "feat(perf): release-gated perf job uploading to Bencher

Add a perf job to release.yml (needs: release, alpha/prod only) that runs
on the self-hosted [gmountie-perf] runner, executes scripts/perf/run.sh,
uploads results to Bencher (branch master, testbed gmountie-perf-pod,
hash = release SHA), and archives the raw outputs as artifacts."
```

---

## Task 9: Docs + spec status

**Files:**
- Modify: `docs/perf/README.md`
- Modify: `docs/superpowers/specs/2026-05-24-perf-tracking-bencher-design.md`

- [ ] **Step 1: Document the Bencher pipeline + runbook**

Add a `## Continuous tracking (Bencher)` section to `docs/perf/README.md` covering:
- What runs on release (the `perf` job; alpha/prod only) and where results live (Bencher Cloud, testbed `gmountie-perf-pod`, branch `master`).
- The two profiles (`lan`, `wan` = `delay 25ms 5ms rate 100Mbit`; ~50ms RTT on loopback) and that `scripts/perf/profile.sh` is the single source of truth.
- How to reproduce locally on the runner: `task perf:ci` (set `BENCHER=1` + token to upload).
- The `WORKDIR` env (set to the node-local PV mount in the workflow, e.g. `/mnt/perf`) must be a real block-backed filesystem — fio's `direct=1` and the disk-floor measurement assume it isn't tmpfs/overlay. This is the contract with the infra repo; both the bench data dir (`$TMPDIR`) and the fio probe live under it.
- The measures: `latency`, `throughput`, `throughput_pct_of_raw` (sequential only), and the `_substrate/*` floor series.
- **Drift runbook** ("the dashboard jumped — what to check"): inspect the `_substrate/*` series first; if those moved, the floor drifted (disk filling, node kernel upgrade, reschedule) — the regression is environmental, not code. If `_substrate/*` is flat but `throughput`/`latency` moved, it's a real gMountie change. If substrate variance is consistently too broad, switch the runner to a kubevirt VM and register it as a new testbed `gmountie-perf-vm`.

- [ ] **Step 2: Flip spec status**

In `docs/superpowers/specs/2026-05-24-perf-tracking-bencher-design.md`, change the Status line to:

```markdown
**Status:** Design approved 2026-05-24; in-repo pipeline implemented 2026-05-25 (runner/ARC/Bencher provisioning tracked in the infra repo).
```

- [ ] **Step 3: Commit**

```bash
git add docs/perf/README.md docs/superpowers/specs/2026-05-24-perf-tracking-bencher-design.md
git commit -m "docs(perf): document Bencher pipeline and drift runbook

Describe the release-gated Bencher tracking, the lan/wan profiles, local
reproduction via task perf:ci, the emitted measures, and a runbook for
distinguishing substrate drift from real regressions. Mark the spec's
in-repo pipeline implemented."
```

---

## Self-Review

**Spec coverage:**
- Trigger / release-gated job → Task 8. ✓
- LAN/WAN codified profiles over loopback TCP → Task 5 (`profile.sh`), Task 6 (`GMOUNTIE_BENCH_TCP=1`). ✓
- Substrate fingerprint (cpu/disk/net) + RTT assertion → Tasks 4/6. ✓
- Substrate fio targets the *same* filesystem as the benches (spec invariant) → Task 6 pins `TMPDIR` + `SUBSTRATE_DIR` together; `WORKDIR` set to the local PV in Task 8. ✓
- Toolchain version assertions (spec runner-upkeep mitigation) → Task 6 `assert_version` against `EXPECT_*`. ✓
- BMF with latency/throughput/normalized + `_substrate/*` series → Tasks 1–3. ✓
- Branch `master` + `--hash`, testbed `gmountie-perf-pod`, `bencher run --adapter json --file` → Tasks 6/8. ✓
- Artifacts → Task 8. ✓
- Regression = Bencher thresholds, advisory (no `--error-on-alert`) → Task 8 (intentionally omits the flag). ✓
- Local harness untouched; layers on top → all new files; `Taskfile` only adds targets. ✓
- Docs + runbook → Task 9. ✓
- Out of scope (image/ARC/node/Bencher account) → not in any task, per spec scope note. ✓

**Placeholder scan:** No TBD/TODO; every code step has complete code; commands have expected output. Step 1 of Task 9 describes prose doc content (acceptable — it's documentation, not code), with the exact section name and bullet list to write.

**Type consistency:** `GoBenchResult`, `Substrate`/`DiskSubstrate`/`NetProbe`, `FioJob`/`FioSide`, `Metric`/`Report`, and functions `ParseGoBench`/`ParseFio`/`ParseIperf3MBs`/`ParsePingAvgRTT`/`BuildReport` are defined in Tasks 1–3 and used consistently by the CLI in Task 4. Substrate JSON field names (`cpu_ops_per_sec`, `disk.seq_read_mbs`, `net.<profile>.rtt_ms`/`bandwidth_mbs`) match between `substrate.go` tags, the `perfbmf substrate` output, and the `run.sh` fixture. fio jobnames (`seqread`/`seqwrite`/`randread`/`randwrite`) match between `substrate.fio` and `cmd/perfbmf` map keys. ✓
