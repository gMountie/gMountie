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
BenchmarkReaddir100/cold-8	   10000	     45000 ns/op	     256 B/op	       4 allocs/op
PASS
ok  	gmountie/test/e2e/perf	12.345s
`

func (s *BMFSuite) TestParseGoBench() {
	res, err := ParseGoBench(strings.NewReader(sampleBench))
	s.Require().NoError(err)
	s.Require().Len(res, 4)

	// Name has the Benchmark prefix and -GOMAXPROCS suffix stripped.
	s.Equal("SeqRead64MiB", res[0].Name)
	s.InDelta(64864928, res[0].NsPerOp, 0.5)
	s.InDelta(1034.99, res[0].MBPerSec, 0.001)
	s.InDelta(1234, res[0].BytesPerOp, 0.5)
	s.InDelta(10, res[0].AllocsPerOp, 0.5)

	// The second -count repetition is parsed and returned in order.
	s.Equal("SeqRead64MiB", res[1].Name)
	s.InDelta(65000000, res[1].NsPerOp, 0.5)

	// Metadata benchmark: no MB/s field -> MBPerSec stays 0.
	s.Equal("OpenStatClose", res[2].Name)
	s.Zero(res[2].MBPerSec)

	// Sub-benchmark name keeps the slash; only the -GOMAXPROCS suffix is stripped.
	s.Equal("Readdir100/cold", res[3].Name)
}

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

func (s *BMFSuite) TestParsePingAvgRTTMissingLine() {
	_, err := ParsePingAvgRTT(strings.NewReader("no rtt summary here\n"))
	s.Require().Error(err)
}

func (s *BMFSuite) TestParseFioMalformedJSON() {
	_, err := ParseFio(strings.NewReader("{not json}"))
	s.Require().Error(err)
}

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
	s.InDelta(150, lat.Value, 0.01) // mean
	s.Require().NotNil(lat.LowerValue)
	s.Require().NotNil(lat.UpperValue)
	s.InDelta(100, *lat.LowerValue, 0.01) // min
	s.InDelta(200, *lat.UpperValue, 0.01) // max
}

func (s *BMFSuite) TestBuildReportEmitsSubstrateSeries() {
	rep := BuildReport(map[string][]GoBenchResult{}, s.sampleSubstrate())
	s.InDelta(1.5e8, rep["_substrate/cpu_compute"]["ops_per_sec"].Value, 1)
	s.InDelta(2000, rep["_substrate/disk_seq_read"]["throughput"].Value, 0.01)
	s.InDelta(1800, rep["_substrate/disk_seq_write"]["throughput"].Value, 0.01)
	s.InDelta(50.0, rep["_substrate/net_rtt_wan"]["latency"].Value, 0.01)
	s.InDelta(0.05, rep["_substrate/net_rtt_lan"]["latency"].Value, 0.01)
}
