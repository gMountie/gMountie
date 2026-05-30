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
//	perfbmf plots sync [--spec scripts/perf/plots.yaml] [--project p] \
//	    [--dry-run] [--prune]
//	    Reconcile the live Bencher dashboard plots to match the YAML spec.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"go.gmountie.dev/gmountie/test/e2e/perf/bmf"
)

func main() {
	if len(os.Args) < 2 {
		fail("usage: perfbmf <cpuprobe|substrate|emit|plots> ...")
	}
	switch os.Args[1] {
	case "cpuprobe":
		cpuprobe()
	case "substrate":
		substrate(os.Args[2:])
	case "emit":
		emit(os.Args[2:])
	case "plots":
		plots(os.Args[2:])
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
	runtime.KeepAlive(acc)
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
	if *cpu <= 0 {
		fail("--cpu is required (run `perfbmf cpuprobe` to get the value)")
	}

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

// mustOpen opens path for reading. The caller does not close the file; for
// this short-lived CLI the OS reclaims it on exit (and parsers read it fully).
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
