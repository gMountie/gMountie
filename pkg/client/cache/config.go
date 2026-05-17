package cache

import (
	"time"

	clientconfig "gmountie/pkg/client/config"
)

// Config is the in-process Config the cache layer consumes. ClientConfig's
// CacheConfig is the operator-facing surface; ConfigFromClient adapts it.
type Config struct {
	MemoryMaxBytes int
	DiskMaxBytes   int
	Path           string
	ChunkSizeBytes int
	AttrTTL        time.Duration
	DirTTL         time.Duration
	NegativeTTL    time.Duration
}

// ConfigFromClient builds a runtime Config from the operator-facing
// CacheConfig. Caller is responsible for checking Enabled before
// constructing the decorator.
//
// Sub-spec C Task 8: clientconfig.CacheConfig still has MaxSizeBytes
// (renamed to MemoryMaxBytes in Task 9). For now, MaxSizeBytes maps
// to MemoryMaxBytes and DiskMaxBytes/Path use zero defaults — Task 9
// adds the real fields and ConfigFromClient updates accordingly.
func ConfigFromClient(cfg clientconfig.CacheConfig) Config {
	return Config{
		MemoryMaxBytes: cfg.MaxSizeBytes,
		DiskMaxBytes:   0,
		Path:           "",
		ChunkSizeBytes: cfg.ChunkSizeBytes,
		AttrTTL:        cfg.AttrTTL,
		DirTTL:         cfg.DirTTL,
		NegativeTTL:    cfg.NegativeTTL,
	}
}
