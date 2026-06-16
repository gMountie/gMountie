package config

import (
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

const (
	// DefaultRpcTimeoutMeta is the default per-RPC timeout for metadata
	// operations (Lookup, GetAttr, Readdir, etc.). Small ops over the network
	// should be cheap.
	DefaultRpcTimeoutMeta = 5 * time.Second
	// DefaultRpcTimeoutIO is the default per-RPC timeout for data operations
	// (Read, Write). Tuned for moderate-sized payloads over an internet link.
	DefaultRpcTimeoutIO = 30 * time.Second
	// DefaultReadaheadChunkBytes is the default size of a single readahead
	// fetch. 1 MiB matches the default FUSE max-write / server frame size, so
	// each prefetch is one server frame and the server's frame-buffer pool is
	// sized for it. Capped down by the Version handshake if the server
	// advertises a smaller frame.
	DefaultReadaheadChunkBytes = 1 << 20
	// DefaultReadaheadThreshold is the number of strictly-sequential reads
	// required before the client arms prefetches.
	DefaultReadaheadThreshold = 3
	// DefaultReadaheadWindow is the number of readahead chunks kept in flight
	// ahead of the cursor. Each in-flight chunk is one concurrent Read RPC, so
	// the window is how many bytes stay overlapped over the link. A shallow
	// window stutters under WAN RTT: the throughput investigation measured a
	// window of 4 reaching only ~71% of a 100 Mbit / 50 ms link, vs ~82% at 16
	// (32 gave no further gain). 16 × 1 MiB = 16 MiB in flight per sequential
	// reader — harmless on LAN (RTT≈0 so little is actually in flight) and the
	// sweet spot on WAN. The knob ranges [1,64] for operators on extreme pipes.
	DefaultReadaheadWindow = 16
	// DefaultInitialConnWindowBytes / DefaultInitialStreamWindowBytes pin the
	// gRPC HTTP/2 flow-control windows (connection + per-stream). At high BDP
	// (e.g. 1 Gbit / 50 ms ≈ 6.25 MB) gRPC-go's auto-tuned window stays tighter
	// than the link — a single sustained stream measured ~36 MiB/s vs the ~52
	// MiB/s a raw TCP flow achieves — so we pin generous windows that cover the
	// worst-case BDP we target (~1 Gbit × ~130 ms). This lifted a 1 Gbit single
	// stream +31% and left a 100 Mbit link unchanged (the window is unused
	// receive credit there; actual buffering tracks in-flight bytes ≈ BDP, and
	// the connection window caps the aggregate). 0 disables pinning and restores
	// gRPC BDP autotuning. NB: gRPC ignores values below the 64 KiB HTTP/2 floor.
	DefaultInitialConnWindowBytes   = 16 << 20
	DefaultInitialStreamWindowBytes = 8 << 20
	// DefaultWriteCoalesceBytes is the per-fd small-write coalescing
	// threshold. Small contiguous writes accumulate until the buffer
	// reaches this size (or Flush/Release/Fsync drains it). 0 disables
	// coalescing.
	DefaultWriteCoalesceBytes = 1 << 20
	// DefaultMaxMessageBytes is the default cap for inbound/outbound gRPC
	// message sizes on the client side. Mirrors the server default so a
	// well-configured pair never trips its own limits.
	DefaultMaxMessageBytes = 16 << 20
	// DefaultKeepaliveTime is how often the client pings an otherwise idle
	// connection to verify liveness.
	DefaultKeepaliveTime = 30 * time.Second
	// DefaultKeepaliveTimeout bounds how long the client waits for a ping
	// ack before tearing the connection down.
	DefaultKeepaliveTimeout = 10 * time.Second
	// DefaultKeepalivePermitWithoutStream lets the client ping even when no
	// RPCs are in flight — required to surface a dead server from an idle
	// FUSE mount.
	DefaultKeepalivePermitWithoutStream = true
	// DefaultCompression is the default gRPC compressor name.
	//
	// Empirically (pprof of a 1 GiB loopback write): snappy.encodeBlock +
	// the surrounding memclr burned ~53% of client CPU and held throughput
	// at 24 MiB/s — i.e., on a fast link, compression IS the bottleneck.
	// Off by default. WAN users on bandwidth-constrained links should
	// flip rpc.compression: snappy in their client config.
	DefaultCompression = CompressionNone
	// DefaultRpcRetryWindow is the total wall-clock budget for retrying a single
	// FS operation through transient failures. Within it, attempts retry with a
	// fresh per-attempt deadline; past it the last error reaches userspace. 0
	// disables retrying (a single attempt — fail fast). Aligned with the server
	// session grace period so transparent resume holds for the whole window.
	DefaultRpcRetryWindow = 60 * time.Second
)

// Valid values for RpcConfig.Compression.
const (
	CompressionNone   = "none"
	CompressionSnappy = "snappy"
)

// ClientKeepaliveConfig holds the gRPC client-side keepalive parameters.
// Clients don't enforce a MinTime — that's a server-only knob.
type ClientKeepaliveConfig struct {
	// Time is how often the client pings an idle connection.
	Time time.Duration `mapstructure:"time" validate:"gte=1s"`
	// Timeout is how long the client waits for a ping ack before
	// closing the connection.
	Timeout time.Duration `mapstructure:"timeout" validate:"gte=1s"`
	// PermitWithoutStream allows the client to ping when no streams are
	// in flight. Must be enabled to detect a dead server from an idle mount.
	PermitWithoutStream bool `mapstructure:"permit_without_stream"`
}

// RpcConfig holds per-RPC client-side timeouts and retry tuning for the FUSE
// client. The per-attempt timeouts bound a single try; RetryWindow bounds how
// long an op retries transient failures. Further retry knobs (attempt counts,
// backoff) stay hardcoded in retry.go until there is evidence operators need
// to tune them.
type RpcConfig struct {
	// TimeoutMeta bounds each metadata RPC (Lookup, GetAttr, Readdir, StatFs,
	// Access, *XAttr, and the mutating metadata ops Mkdir/Rmdir/Rename/...).
	TimeoutMeta time.Duration `mapstructure:"timeout_meta" validate:"required,gte=1ms"`
	// TimeoutIO bounds each data RPC (Read, Write, and file-state ops like
	// Flush/Fsync/Release/locking/Allocate).
	TimeoutIO time.Duration `mapstructure:"timeout_io" validate:"required,gte=1ms"`
	// ReadaheadChunkBytes is the size of a single readahead fetch. Zero
	// means "no readahead".
	ReadaheadChunkBytes int `mapstructure:"readahead_chunk_bytes" validate:"gte=0"`
	// ReadaheadThreshold is the number of strictly-sequential reads
	// required before the client arms a one-chunk-ahead prefetch.
	ReadaheadThreshold int `mapstructure:"readahead_threshold" validate:"min=1,max=16"`
	// ReadaheadWindow is how many ReadaheadChunkBytes chunks to keep
	// prefetched/in-flight ahead of a sequential reader. 1 preserves the
	// legacy single-chunk behaviour.
	ReadaheadWindow int `mapstructure:"readahead_window" validate:"min=1,max=64"`
	// WriteCoalesceBytes caps the per-fd small-write coalescing buffer.
	// Writes >= this size pass through directly; smaller contiguous writes
	// accumulate until the buffer reaches this size or Flush/Release
	// drains it. 0 disables coalescing.
	WriteCoalesceBytes int `mapstructure:"write_coalesce_bytes" validate:"min=0,max=16777216"`
	// MaxMessageBytes caps both inbound and outbound gRPC message sizes on
	// the client. Mirror of the server-side cap; same [64 KiB, 64 MiB] range.
	MaxMessageBytes int `mapstructure:"max_message_bytes" validate:"min=65536,max=67108864"`
	// InitialConnWindowBytes / InitialStreamWindowBytes pin the gRPC HTTP/2
	// connection- and stream-level flow-control receive windows. Pinning a
	// generous window (≥ the worst-case bandwidth-delay product) lets a single
	// connection fill a high-BDP link that gRPC's autotuner under-serves; 0
	// leaves autotuning on. Values below the 64 KiB HTTP/2 floor are ignored by
	// gRPC. Capped at 1 GiB. See Default*WindowBytes for the why.
	InitialConnWindowBytes   int `mapstructure:"initial_conn_window_bytes" validate:"min=0,max=1073741824"`
	InitialStreamWindowBytes int `mapstructure:"initial_stream_window_bytes" validate:"min=0,max=1073741824"`
	// Keepalive controls gRPC HTTP/2 keepalive pings on the client side.
	Keepalive ClientKeepaliveConfig `mapstructure:"keepalive"`
	// Compression names the gRPC compressor to apply to every RPC on this
	// connection. "none" disables compression entirely; "snappy" uses the
	// snappy codec registered in pkg/common/grpc/snappy. Default "none" —
	// see DefaultCompression for the why.
	Compression string `mapstructure:"compression" validate:"oneof=none snappy"`
	// RetryWindow bounds how long a single FS op retries transient failures
	// (Unavailable / DeadlineExceeded) before the error surfaces. 0 = fail fast.
	RetryWindow time.Duration `mapstructure:"retry_window" validate:"gte=0"`
}

// defaultRpcConfig returns an RpcConfig seeded entirely from the Default*
// constants. It is the single source of the literal defaults, reused by the
// v==nil fast path and as the unmarshal target so the SetDefault block below
// (kept only to surface defaults via v.AllSettings()/--effective) and this
// literal can never drift apart.
func defaultRpcConfig() *RpcConfig {
	return &RpcConfig{
		TimeoutMeta:              DefaultRpcTimeoutMeta,
		TimeoutIO:                DefaultRpcTimeoutIO,
		ReadaheadChunkBytes:      DefaultReadaheadChunkBytes,
		ReadaheadThreshold:       DefaultReadaheadThreshold,
		ReadaheadWindow:          DefaultReadaheadWindow,
		WriteCoalesceBytes:       DefaultWriteCoalesceBytes,
		MaxMessageBytes:          DefaultMaxMessageBytes,
		InitialConnWindowBytes:   DefaultInitialConnWindowBytes,
		InitialStreamWindowBytes: DefaultInitialStreamWindowBytes,
		Keepalive: ClientKeepaliveConfig{
			Time:                DefaultKeepaliveTime,
			Timeout:             DefaultKeepaliveTimeout,
			PermitWithoutStream: DefaultKeepalivePermitWithoutStream,
		},
		Compression: DefaultCompression,
		RetryWindow: DefaultRpcRetryWindow,
	}
}

// NewRpcConfig parses an RpcConfig from a viper sub-tree. A nil v yields
// defaults; an empty sub-tree yields defaults; explicit values override.
func NewRpcConfig(v *viper.Viper) (*RpcConfig, error) {
	cfg := defaultRpcConfig()
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("timeout_meta", DefaultRpcTimeoutMeta)
	v.SetDefault("timeout_io", DefaultRpcTimeoutIO)
	v.SetDefault("readahead_chunk_bytes", DefaultReadaheadChunkBytes)
	v.SetDefault("readahead_threshold", DefaultReadaheadThreshold)
	v.SetDefault("readahead_window", DefaultReadaheadWindow)
	v.SetDefault("write_coalesce_bytes", DefaultWriteCoalesceBytes)
	v.SetDefault("max_message_bytes", DefaultMaxMessageBytes)
	v.SetDefault("initial_conn_window_bytes", DefaultInitialConnWindowBytes)
	v.SetDefault("initial_stream_window_bytes", DefaultInitialStreamWindowBytes)
	v.SetDefault("keepalive.time", DefaultKeepaliveTime)
	v.SetDefault("keepalive.timeout", DefaultKeepaliveTimeout)
	v.SetDefault("keepalive.permit_without_stream", DefaultKeepalivePermitWithoutStream)
	v.SetDefault("compression", DefaultCompression)
	v.SetDefault("retry_window", DefaultRpcRetryWindow)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, err
	}
	return cfg, nil
}
