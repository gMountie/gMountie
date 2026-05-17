package cache

import (
	"time"

	clientconfig "gmountie/pkg/client/config"
)

// Config is the in-process Config the cache layer consumes. ClientConfig's
// CacheConfig is the operator-facing surface; ConfigFromClient adapts it.
type Config struct {
	MaxSizeBytes   int
	ChunkSizeBytes int
	AttrTTL        time.Duration
	DirTTL         time.Duration
	NegativeTTL    time.Duration
}

// ConfigFromClient builds a runtime Config from the operator-facing
// CacheConfig. Caller is responsible for checking Enabled before
// constructing the decorator.
func ConfigFromClient(cfg clientconfig.CacheConfig) Config {
	return Config{
		MaxSizeBytes:   cfg.MaxSizeBytes,
		ChunkSizeBytes: cfg.ChunkSizeBytes,
		AttrTTL:        cfg.AttrTTL,
		DirTTL:         cfg.DirTTL,
		NegativeTTL:    cfg.NegativeTTL,
	}
}
