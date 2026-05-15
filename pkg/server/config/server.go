package config

const (
	// DefaultAddress is the default address that the server will listen on
	DefaultAddress = "0.0.0.0"
	// DefaultPort is the default port that the server will listen on
	DefaultPort = 9449
	// DefaultMetricsAddr is the default address the ops HTTP server
	// (/metrics, /healthz, /readyz, /version) listens on.
	DefaultMetricsAddr = ":9090"
	// DefaultFrameSizeBytes is the default chunk size for server-streamed
	// reads. One frame per ReadStreamer iteration; the client accumulates
	// frames into the caller-supplied buffer. 1 MiB balances per-RPC
	// allocation cost against keeping each frame well under the default 4 MiB
	// gRPC message ceiling.
	DefaultFrameSizeBytes = 1 << 20
	// DefaultCompoundMaxParallel bounds the in-flight sub-ops the
	// CompoundDispatcher will dispatch concurrently for a single Compound RPC.
	// 8 keeps tail latency reasonable on slow links without flooding the
	// volume filesystem with parallel metadata syscalls.
	DefaultCompoundMaxParallel = 8
)

// ServerConfig is a struct that holds the configuration for the server
type ServerConfig struct {
	// Address is the address that the server will listen on
	Address string `validate:"required,ip"`
	// Port is the port that the server will listen on
	Port uint `validate:"required"`
	// Metrics enables the ops HTTP server.
	Metrics bool
	// MetricsAddr is the address the ops HTTP server listens on.
	MetricsAddr string `validate:"hostname_port" mapstructure:"metrics_addr"`
	// FrameSizeBytes bounds each ReadFrame emitted by the server's streaming
	// Read. Capped at 16 MiB to stay safely under gRPC's max recv size; floor
	// of 4 KiB matches the typical page size.
	FrameSizeBytes int `validate:"min=4096,max=16777216" mapstructure:"frame_size_bytes"`
	// CompoundMaxParallel caps the concurrent sub-ops in flight for a single
	// Compound RPC. Defaults to DefaultCompoundMaxParallel; the upper bound is
	// a sanity cap, not a tuned value.
	CompoundMaxParallel int `validate:"min=1,max=256" mapstructure:"compound_max_parallel"`
}
