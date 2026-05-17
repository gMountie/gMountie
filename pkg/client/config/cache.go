package config

import (
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

const (
	// DefaultCacheEnabled gates the cache decorator. Disabled by default in
	// Phase 4 Sub-spec B; Sub-spec C flips the default once persistence
	// proves the disk side of the story.
	DefaultCacheEnabled = false
	// DefaultCacheMaxSizeBytes is the total byte budget across all three
	// sub-caches (attr + dir + data). Eviction is global LRU once this is
	// exceeded. 1 GiB.
	DefaultCacheMaxSizeBytes = 1 << 30
	// DefaultCacheChunkSizeBytes is the data cache's chunk granularity.
	// Reads are split into chunk-sized requests against the inner backend
	// on a miss. 1 MiB.
	DefaultCacheChunkSizeBytes = 1 << 20
	// DefaultCacheAttrTTL is the per-entry lifetime for positive attribute
	// cache hits.
	DefaultCacheAttrTTL = 5 * time.Second
	// DefaultCacheDirTTL is the per-entry lifetime for directory listing
	// cache hits.
	DefaultCacheDirTTL = 5 * time.Second
	// DefaultCacheNegativeTTL is the per-entry lifetime for negative
	// attribute cache entries (paths that returned ENOENT). Short by
	// design so deletions elsewhere become visible quickly.
	DefaultCacheNegativeTTL = 2 * time.Second
)

// CacheConfig governs the optional client-side in-memory cache layer that
// decorates the gRPC FileSystemBackend. Disabled by default in Phase 4
// Sub-spec B; Sub-spec C flips the default once persistence proves the
// disk side.
type CacheConfig struct {
	// Enabled gates whether the cache decorator is inserted at mount time.
	// false (the default) keeps the chain identical to Sub-spec A.
	Enabled bool `mapstructure:"enabled"`
	// MaxSizeBytes is the total byte budget across all three sub-caches
	// (attr + dir + data). Eviction is global LRU once this is exceeded.
	// Pinned to [0, 64 GiB].
	MaxSizeBytes int `mapstructure:"max_size_bytes" validate:"min=0,max=68719476736"`
	// ChunkSizeBytes is the data cache's chunk granularity. Reads are split
	// into chunk-sized requests against the inner backend on a miss. Pinned
	// to [4 KiB, 16 MiB].
	ChunkSizeBytes int `mapstructure:"chunk_size_bytes" validate:"min=4096,max=16777216"`
	// AttrTTL is the per-entry lifetime for positive attribute cache hits.
	AttrTTL time.Duration `mapstructure:"attr_ttl"`
	// DirTTL is the per-entry lifetime for directory listing cache hits.
	DirTTL time.Duration `mapstructure:"dir_ttl"`
	// NegativeTTL is the per-entry lifetime for negative attribute cache
	// entries (paths that returned ENOENT). Short by design so deletions
	// elsewhere become visible quickly.
	NegativeTTL time.Duration `mapstructure:"negative_ttl"`
}

// NewCacheConfig parses a CacheConfig from a viper sub-tree. A nil v
// yields defaults; an empty sub-tree yields defaults; explicit values
// override.
func NewCacheConfig(v *viper.Viper) (*CacheConfig, error) {
	cfg := &CacheConfig{
		Enabled:        DefaultCacheEnabled,
		MaxSizeBytes:   DefaultCacheMaxSizeBytes,
		ChunkSizeBytes: DefaultCacheChunkSizeBytes,
		AttrTTL:        DefaultCacheAttrTTL,
		DirTTL:         DefaultCacheDirTTL,
		NegativeTTL:    DefaultCacheNegativeTTL,
	}
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("enabled", DefaultCacheEnabled)
	v.SetDefault("max_size_bytes", DefaultCacheMaxSizeBytes)
	v.SetDefault("chunk_size_bytes", DefaultCacheChunkSizeBytes)
	v.SetDefault("attr_ttl", DefaultCacheAttrTTL)
	v.SetDefault("dir_ttl", DefaultCacheDirTTL)
	v.SetDefault("negative_ttl", DefaultCacheNegativeTTL)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, err
	}
	return cfg, nil
}
