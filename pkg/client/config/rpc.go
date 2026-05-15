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
	// fetch issued by the client behind a sequential-read kernel hint. The
	// readahead path is wired up by a later phase task; the value is declared
	// here so config plumbing can ship ahead of the consumer.
	DefaultReadaheadChunkBytes = 64 << 10
)

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
	// ReadaheadChunkBytes is the size of a single readahead fetch. Declared
	// here so config plumbing can ship ahead of the consumer; wired up by a
	// later phase task. Zero means "no readahead", which is the current
	// behaviour.
	ReadaheadChunkBytes int `mapstructure:"readahead_chunk_bytes" validate:"gte=0"`
}

// NewRpcConfig parses an RpcConfig from a viper sub-tree. A nil v yields
// defaults; an empty sub-tree yields defaults; explicit values override.
func NewRpcConfig(v *viper.Viper) (*RpcConfig, error) {
	cfg := &RpcConfig{
		TimeoutMeta:         DefaultRpcTimeoutMeta,
		TimeoutIO:           DefaultRpcTimeoutIO,
		ReadaheadChunkBytes: DefaultReadaheadChunkBytes,
	}
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("timeout_meta", DefaultRpcTimeoutMeta)
	v.SetDefault("timeout_io", DefaultRpcTimeoutIO)
	v.SetDefault("readahead_chunk_bytes", DefaultReadaheadChunkBytes)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, err
	}
	return cfg, nil
}
