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
	// ahead of the cursor. 4 is a bandwidth-delay-product start for ~50 ms RTT
	// / 100 Mbit; the knob ranges [1,64] so operators on longer/fatter pipes
	// raise it. Each in-flight chunk is one concurrent Read RPC.
	DefaultReadaheadWindow = 4
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

// RpcConfig holds per-RPC client-side timeouts and (in future plans) retry
// tuning. Retry parameters are intentionally hardcoded in retry.go for now —
// add config keys here only when we have evidence we need them.
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
	// Keepalive controls gRPC HTTP/2 keepalive pings on the client side.
	Keepalive ClientKeepaliveConfig `mapstructure:"keepalive"`
	// Compression names the gRPC compressor to apply to every RPC on this
	// connection. "none" disables compression entirely; "snappy" uses the
	// snappy codec registered in pkg/server/grpc/snappy. Default "none" —
	// see DefaultCompression for the why.
	Compression string `mapstructure:"compression" validate:"oneof=none snappy"`
}

// NewRpcConfig parses an RpcConfig from a viper sub-tree. A nil v yields
// defaults; an empty sub-tree yields defaults; explicit values override.
func NewRpcConfig(v *viper.Viper) (*RpcConfig, error) {
	cfg := &RpcConfig{
		TimeoutMeta:         DefaultRpcTimeoutMeta,
		TimeoutIO:           DefaultRpcTimeoutIO,
		ReadaheadChunkBytes: DefaultReadaheadChunkBytes,
		ReadaheadThreshold:  DefaultReadaheadThreshold,
		ReadaheadWindow:     DefaultReadaheadWindow,
		WriteCoalesceBytes:  DefaultWriteCoalesceBytes,
		MaxMessageBytes:     DefaultMaxMessageBytes,
		Keepalive: ClientKeepaliveConfig{
			Time:                DefaultKeepaliveTime,
			Timeout:             DefaultKeepaliveTimeout,
			PermitWithoutStream: DefaultKeepalivePermitWithoutStream,
		},
		Compression: DefaultCompression,
	}
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
	v.SetDefault("keepalive.time", DefaultKeepaliveTime)
	v.SetDefault("keepalive.timeout", DefaultKeepaliveTimeout)
	v.SetDefault("keepalive.permit_without_stream", DefaultKeepalivePermitWithoutStream)
	v.SetDefault("compression", DefaultCompression)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, err
	}
	return cfg, nil
}
