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

// pingRTT matches the Linux/iputils ping summary ("rtt min/avg/max/mdev = ...")
// and captures the avg field. macOS/BSD emit "round-trip", not "rtt"; gMountie
// targets Linux only, so that variant is intentionally unsupported.
var pingRTT = regexp.MustCompile(`rtt [^=]*= [0-9.]+/([0-9.]+)/`)

// ParsePingAvgRTT extracts the average RTT (ms) from `ping` summary output.
func ParsePingAvgRTT(r io.Reader) (float64, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("read ping output: %w", err)
	}
	m := pingRTT.FindSubmatch(b)
	if m == nil {
		return 0, fmt.Errorf("no rtt summary line in ping output")
	}
	return atof(string(m[1])), nil
}
