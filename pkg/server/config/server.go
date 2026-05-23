package config

import "time"

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
	// DefaultMaxMessageBytes is the default cap for gRPC inbound/outbound
	// message sizes. 16 MiB sits well above the streaming Read/Write frame
	// size while still rejecting accidental jumbo payloads. Validation pins
	// the configurable range to [64 KiB, 64 MiB].
	DefaultMaxMessageBytes = 16 << 20
	// DefaultKeepaliveTime is how often the server pings an otherwise idle
	// connection to verify liveness.
	DefaultKeepaliveTime = 30 * time.Second
	// DefaultKeepaliveTimeout bounds how long the server waits for a ping ack
	// before tearing the connection down.
	DefaultKeepaliveTimeout = 10 * time.Second
	// DefaultKeepaliveMinTime is the minimum interval the server permits
	// between client keepalive pings. Clients pinging more aggressively get
	// GOAWAY'd.
	DefaultKeepaliveMinTime = 10 * time.Second
	// DefaultKeepalivePermitWithoutStream lets clients ping when there are
	// no active RPCs. Required so an idle FUSE mount still detects a dead
	// server.
	DefaultKeepalivePermitWithoutStream = true
	// DefaultServerSubscribeBufferSize is the per-subscriber channel
	// depth in the event bus. 256 gives roughly 256 invalidation events
	// of headroom before a slow client is considered lagging.
	DefaultServerSubscribeBufferSize = 256
	// DefaultServerSubscribeHeartbeatInterval is how often the event bus
	// emits a HEARTBEAT frame to each subscriber so the client can
	// distinguish an idle stream from a dead one.
	DefaultServerSubscribeHeartbeatInterval = 10 * time.Second
)

// ServerKeepaliveConfig holds the gRPC server-side keepalive parameters
// and matching enforcement policy. Defaults are tuned to detect dead
// connections within ~Time+Timeout while tolerating idle FUSE mounts.
type ServerKeepaliveConfig struct {
	// Time is how often the server pings an idle connection.
	Time time.Duration `mapstructure:"time" validate:"gte=1s"`
	// Timeout is how long the server waits for a ping ack before
	// closing the connection.
	Timeout time.Duration `mapstructure:"timeout" validate:"gte=1s"`
	// MinTime is the minimum interval between client pings the server will
	// tolerate. Clients pinging faster than this get GOAWAY'd.
	MinTime time.Duration `mapstructure:"min_time" validate:"gte=1s"`
	// PermitWithoutStream allows clients to ping even when no streams are
	// in flight. Must be enabled to detect dead servers from idle mounts.
	PermitWithoutStream bool `mapstructure:"permit_without_stream"`
}

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
	// Pprof exposes /debug/pprof/* on the ops HTTP server. Off by
	// default: pprof endpoints leak goroutine names + symbols and can
	// stall the runtime on large captures, so they have no business
	// being on a production listener. Flip true for perf investigations.
	Pprof bool `mapstructure:"pprof"`
	// FrameSizeBytes bounds each ReadFrame emitted by the server's streaming
	// Read. Capped at 16 MiB to stay safely under gRPC's max recv size; floor
	// of 4 KiB matches the typical page size.
	FrameSizeBytes int `validate:"min=4096,max=16777216" mapstructure:"frame_size_bytes"`
	// CompoundMaxParallel caps the concurrent sub-ops in flight for a single
	// Compound RPC. Defaults to DefaultCompoundMaxParallel; the upper bound is
	// a sanity cap, not a tuned value.
	CompoundMaxParallel int `validate:"min=1,max=256" mapstructure:"compound_max_parallel"`
	// MaxMessageBytes caps both inbound and outbound gRPC message sizes.
	// Range pins values to [64 KiB, 64 MiB] — small enough to refuse runaway
	// requests, large enough to handle the streaming Read/Write frame plus
	// header overhead.
	MaxMessageBytes int `validate:"min=65536,max=67108864" mapstructure:"max_message_bytes"`
	// Keepalive controls gRPC HTTP/2 keepalive pings and enforcement.
	Keepalive ServerKeepaliveConfig `mapstructure:"keepalive"`
	// SubscribeBufferSize is the per-subscriber channel depth in the
	// event bus. Larger values tolerate bursty invalidation storms at
	// the cost of memory; a slow subscriber that fills the buffer is
	// dropped and must reconnect.
	SubscribeBufferSize int `validate:"min=1" mapstructure:"subscribe_buffer_size"`
	// SubscribeHeartbeatInterval controls how often the event bus emits
	// a HEARTBEAT to each live subscriber. Clients use the absence of
	// heartbeats to detect a stale stream and trigger reconnection.
	SubscribeHeartbeatInterval time.Duration `mapstructure:"subscribe_heartbeat_interval"`
}
