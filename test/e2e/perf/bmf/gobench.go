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
	Name        string // benchmark name, "Benchmark" prefix and -GOMAXPROCS suffix stripped
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
		// Intentionally permissive: a real benchmark line has ≥4 fields, but
		// shorter Benchmark-prefixed lines are harmless — atof returns 0 for any
		// non-numeric field, so metrics simply stay zero-valued.
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
