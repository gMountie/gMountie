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
	// fetch issued by the client behind a sequential-read kernel hint.
	DefaultReadaheadChunkBytes = 64 << 10
	// DefaultReadaheadThreshold is the number of strictly-sequential reads
	// required before the client arms a one-chunk-ahead prefetch.
	DefaultReadaheadThreshold = 3
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
	// MaxMessageBytes caps both inbound and outbound gRPC message sizes on
	// the client. Mirror of the server-side cap; same [64 KiB, 64 MiB] range.
	MaxMessageBytes int `mapstructure:"max_message_bytes" validate:"min=65536,max=67108864"`
	// Keepalive controls gRPC HTTP/2 keepalive pings on the client side.
	Keepalive ClientKeepaliveConfig `mapstructure:"keepalive"`
}

// NewRpcConfig parses an RpcConfig from a viper sub-tree. A nil v yields
// defaults; an empty sub-tree yields defaults; explicit values override.
func NewRpcConfig(v *viper.Viper) (*RpcConfig, error) {
	cfg := &RpcConfig{
		TimeoutMeta:         DefaultRpcTimeoutMeta,
		TimeoutIO:           DefaultRpcTimeoutIO,
		ReadaheadChunkBytes: DefaultReadaheadChunkBytes,
		ReadaheadThreshold:  DefaultReadaheadThreshold,
		MaxMessageBytes:     DefaultMaxMessageBytes,
		Keepalive: ClientKeepaliveConfig{
			Time:                DefaultKeepaliveTime,
			Timeout:             DefaultKeepaliveTimeout,
			PermitWithoutStream: DefaultKeepalivePermitWithoutStream,
		},
	}
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("timeout_meta", DefaultRpcTimeoutMeta)
	v.SetDefault("timeout_io", DefaultRpcTimeoutIO)
	v.SetDefault("readahead_chunk_bytes", DefaultReadaheadChunkBytes)
	v.SetDefault("readahead_threshold", DefaultReadaheadThreshold)
	v.SetDefault("max_message_bytes", DefaultMaxMessageBytes)
	v.SetDefault("keepalive.time", DefaultKeepaliveTime)
	v.SetDefault("keepalive.timeout", DefaultKeepaliveTimeout)
	v.SetDefault("keepalive.permit_without_stream", DefaultKeepalivePermitWithoutStream)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, err
	}
	return cfg, nil
}
