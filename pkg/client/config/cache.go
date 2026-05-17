package config

import (
	"path/filepath"
	"time"

	"github.com/adrg/xdg"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

const (
	// DefaultCacheEnabled gates the cache decorator. Flipped to true
	// in Sub-spec C now that persistence proves the disk side.
	DefaultCacheEnabled = true
	// DefaultCacheMemoryMaxBytes caps the in-memory tier across all
	// three sub-caches. 256 MiB.
	DefaultCacheMemoryMaxBytes = 256 << 20
	// DefaultCacheDiskMaxBytes caps chunks/ + bbolt approx. 10 GiB.
	DefaultCacheDiskMaxBytes = 10 << 30
	// DefaultCacheChunkSizeBytes is the data cache's chunk granularity.
	// 1 MiB.
	DefaultCacheChunkSizeBytes = 1 << 20
	// DefaultCacheAttrTTL is the per-entry lifetime for positive
	// attribute cache hits.
	DefaultCacheAttrTTL = 5 * time.Second
	// DefaultCacheDirTTL is the per-entry lifetime for directory
	// listing cache hits.
	DefaultCacheDirTTL = 5 * time.Second
	// DefaultCacheNegativeTTL is the per-entry lifetime for negative
	// attribute cache entries. Short by design so deletions elsewhere
	// become visible quickly.
	DefaultCacheNegativeTTL = 2 * time.Second
)

// defaultCachePath returns the XDG-default cache directory.
func defaultCachePath() string {
	return filepath.Join(xdg.CacheHome, "gmountie")
}

// CacheConfig governs the client-side cache. Sub-spec B introduced the
// in-memory tier; Sub-spec C adds persistence under Path with two
// independent byte caps. Disabled-by-default in B, enabled-by-default
// in C.
type CacheConfig struct {
	// Enabled gates whether the cache decorator is inserted at mount time.
	Enabled bool `mapstructure:"enabled"`
	// Path is the per-mount cache root. Each volume gets a per-volume
	// subdirectory under it holding a flock-based LOCK file, the bbolt
	// meta.db, and the chunks/ tree.
	Path string `mapstructure:"path"`
	// MemoryMaxBytes is the in-memory tier byte budget across attr+dir+
	// data sub-caches. Pinned to [0, 64 GiB].
	MemoryMaxBytes int `mapstructure:"memory_max_bytes" validate:"min=0,max=68719476736"`
	// DiskMaxBytes is the on-disk chunks/ byte budget. 0 = unbounded.
	DiskMaxBytes int `mapstructure:"disk_max_bytes" validate:"min=0"`
	// ChunkSizeBytes is the data cache's chunk granularity. Reads are
	// split into chunk-sized requests against the inner backend on a
	// miss. Pinned to [4 KiB, 16 MiB].
	ChunkSizeBytes int `mapstructure:"chunk_size_bytes" validate:"min=4096,max=16777216"`
	// AttrTTL is the per-entry lifetime for positive attribute cache hits.
	AttrTTL time.Duration `mapstructure:"attr_ttl"`
	// DirTTL is the per-entry lifetime for directory listing cache hits.
	DirTTL time.Duration `mapstructure:"dir_ttl"`
	// NegativeTTL is the per-entry lifetime for negative attribute cache
	// entries (paths that returned ENOENT). Short by design.
	NegativeTTL time.Duration `mapstructure:"negative_ttl"`
}

// NewCacheConfig parses a CacheConfig from a viper sub-tree. A nil v
// yields defaults; an empty sub-tree yields defaults; explicit values
// override.
func NewCacheConfig(v *viper.Viper) (*CacheConfig, error) {
	cfg := &CacheConfig{
		Enabled:        DefaultCacheEnabled,
		Path:           defaultCachePath(),
		MemoryMaxBytes: DefaultCacheMemoryMaxBytes,
		DiskMaxBytes:   DefaultCacheDiskMaxBytes,
		ChunkSizeBytes: DefaultCacheChunkSizeBytes,
		AttrTTL:        DefaultCacheAttrTTL,
		DirTTL:         DefaultCacheDirTTL,
		NegativeTTL:    DefaultCacheNegativeTTL,
	}
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("enabled", DefaultCacheEnabled)
	v.SetDefault("path", defaultCachePath())
	v.SetDefault("memory_max_bytes", DefaultCacheMemoryMaxBytes)
	v.SetDefault("disk_max_bytes", DefaultCacheDiskMaxBytes)
	v.SetDefault("chunk_size_bytes", DefaultCacheChunkSizeBytes)
	v.SetDefault("attr_ttl", DefaultCacheAttrTTL)
	v.SetDefault("dir_ttl", DefaultCacheDirTTL)
	v.SetDefault("negative_ttl", DefaultCacheNegativeTTL)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, err
	}
	return cfg, nil
}
