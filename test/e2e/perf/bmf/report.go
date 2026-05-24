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
