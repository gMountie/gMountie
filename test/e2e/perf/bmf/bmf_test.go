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
