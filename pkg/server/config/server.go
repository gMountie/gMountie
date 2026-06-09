package config

import "time"

const (
	// DefaultAddress is the default address that the server will listen on
	DefaultAddress = "0.0.0.0"
	// DefaultPort is the default port that the server will listen on
	DefaultPort = 9449
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
	// DefaultMaxMessageBytes seeds server.grpc.limits.max_recv_message_size:
	// the cap on the largest single gRPC message the server accepts. 16 MiB
	// sits well above the streaming Read/Write frame size while still
	// rejecting accidental jumbo payloads.
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
	// DefaultSessionGracePeriod is how long the server retains a disconnected
	// client's session — its fd table and idempotency cache — before reaping
	// it, so a brief network drop can Resume the same session. Aligned with the
	// client rpc.retry_window default. Cost: a dropped client's fds and POSIX
	// locks are held for this long before release.
	DefaultSessionGracePeriod = 60 * time.Second
)

// SessionConfig controls per-client session retention.
type SessionConfig struct {
	// GracePeriod is how long a disconnected session (fds + idempotency cache)
	// is retained before reaping. Must be >= 1s. Should be >= the client
	// rpc.retry_window (default 60s) so transparent resume holds for the whole
	// window.
	GracePeriod time.Duration `mapstructure:"grace_period" validate:"gte=1s"`
}

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

// TLSConfig controls how the server presents TLS. When CertFile/KeyFile are
// empty and Disabled is false, the server auto-generates a self-signed cert
// on first startup (the SSH host-key pattern — see Phase 7 spec §3.1.1).
type TLSConfig struct {
	CertFile     string `mapstructure:"cert_file"`
	KeyFile      string `mapstructure:"key_file"`
	ClientCAFile string `mapstructure:"client_ca_file"` // mTLS — reserved for PR 3; left unused here
	MinVersion   string `mapstructure:"min_version" validate:"omitempty,oneof=1.2 1.3"`
	Disabled     bool   `mapstructure:"disabled"`
}

// OpsAuthConfig — Type is "none" (default), "basic", or "mtls".
type OpsAuthConfig struct {
	Type  string                `mapstructure:"type" validate:"omitempty,oneof=none basic mtls"`
	Users []BasicAuthConfigUser `mapstructure:"users"`
}

// OpsTLSConfig enables TLS (and, for type: mtls, client-cert verification) on
// the ops listener. Empty CertFile/KeyFile ⇒ plain HTTP (the default).
type OpsTLSConfig struct {
	CertFile     string `mapstructure:"cert_file"`
	KeyFile      string `mapstructure:"key_file"`
	ClientCAFile string `mapstructure:"client_ca_file"`
}

// OpsConfig controls the operational HTTP endpoint (/metrics, /healthz,
// /readyz, /version, /debug/pprof, /ops/acl/reload). Bound to 127.0.0.1:9090
// by default so cluster ops reach it via a sidecar / port-forward; binding to
// a non-loopback addr requires Auth.Type != "none" (enforced at startup).
type OpsConfig struct {
	Addr string        `mapstructure:"addr"`
	Auth OpsAuthConfig `mapstructure:"auth"`
	TLS  OpsTLSConfig  `mapstructure:"tls"`
}

// GRPCConfig controls gRPC server-side behavior that isn't transport
// (TLS) or routing.
type GRPCConfig struct {
	// Reflection enables the gRPC reflection service. Off by default so
	// production deployments don't leak the service surface to anonymous
	// callers; turn on for dev / debug.
	Reflection bool `mapstructure:"reflection"`

	// Limits caps per-connection resource use.
	Limits LimitsConfig `mapstructure:"limits"`
}

// LimitsConfig — defaults aim to absorb a normal client and refuse
// obviously hostile traffic.
type LimitsConfig struct {
	// MaxRecvMessageSize bounds the largest single gRPC message the server
	// accepts (bytes). Default 16 MiB — matches the existing frame_size
	// default so a normal Read/Write frame fits.
	MaxRecvMessageSize int `mapstructure:"max_recv_message_size"`
	// MaxConcurrentStreams bounds the number of in-flight streams per
	// single connection. Default 256 — generous for a real client,
	// bites only on stream-flood attacks.
	MaxConcurrentStreams uint32 `mapstructure:"max_concurrent_streams"`
	// MaxConnectionIdle closes idle connections after this duration.
	// Default 5m. Set to 0 to disable.
	MaxConnectionIdle time.Duration `mapstructure:"max_connection_idle"`
	// MaxConnectionAge hard-caps total connection lifetime. Default 0
	// (unlimited) — long-lived sessions are the gMountie norm.
	MaxConnectionAge time.Duration `mapstructure:"max_connection_age"`
}

// ServerConfig is a struct that holds the configuration for the server
type ServerConfig struct {
	// Address is the address that the server will listen on
	Address string `validate:"required,ip"`
	// Port is the port that the server will listen on
	Port uint `validate:"required"`
	// TLS controls how the server presents TLS to connecting clients.
	TLS TLSConfig `mapstructure:"tls"`
	// Ops controls the operational HTTP endpoint (/metrics, /healthz,
	// /readyz, /version, /debug/pprof).
	Ops OpsConfig `mapstructure:"ops"`
	// Metrics enables the gRPC Prometheus interceptors (the generic
	// grpc_server_* series). It does NOT gate the ops HTTP server — that
	// always runs on Ops.Addr — nor the custom gmountie_server_* collectors.
	Metrics bool
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
	// Keepalive controls gRPC HTTP/2 keepalive pings and enforcement.
	Keepalive ServerKeepaliveConfig `mapstructure:"keepalive"`
	// Session controls per-client session retention (grace period).
	Session SessionConfig `mapstructure:"session"`
	// SubscribeBufferSize is the per-subscriber channel depth in the
	// event bus. Larger values tolerate bursty invalidation storms at
	// the cost of memory; a slow subscriber that fills the buffer is
	// dropped and must reconnect.
	SubscribeBufferSize int `validate:"min=1" mapstructure:"subscribe_buffer_size"`
	// SubscribeHeartbeatInterval controls how often the event bus emits
	// a HEARTBEAT to each live subscriber. Clients use the absence of
	// heartbeats to detect a stale stream and trigger reconnection.
	SubscribeHeartbeatInterval time.Duration `mapstructure:"subscribe_heartbeat_interval"`
	// GRPC controls gRPC server-side behavior (reflection, DoS limits).
	GRPC GRPCConfig `mapstructure:"grpc"`
}
