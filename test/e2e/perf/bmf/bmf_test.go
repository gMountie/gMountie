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
