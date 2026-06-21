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
	// DefaultCacheSubscribeEnabled gates the Subscribe-based push
	// invalidation path. When true the client opens a persistent
	// Subscribe stream to receive server-pushed invalidation events,
	// and the validity tracker starts in the verified state only after
	// the first HEARTBEAT. When false, the cache marks itself globally
	// verified at construction and TTL is the sole freshness signal.
	DefaultCacheSubscribeEnabled = true
	// DefaultCacheMemoryMaxBytes caps the in-memory tier across all
	// three sub-caches. 256 MiB.
	DefaultCacheMemoryMaxBytes = 256 << 20
	// DefaultCacheDiskMaxBytes caps chunks/ + bbolt approx. 10 GiB.
	DefaultCacheDiskMaxBytes = 10 << 30
	// DefaultCacheChunkSizeBytes is the data cache's chunk granularity.
	// 1 MiB.
	DefaultCacheChunkSizeBytes = 1 << 20
	// DefaultCacheAttrTTL is the per-entry lifetime for positive
	// attribute cache hits. Relaxed from 5s to 5min in Sub-spec D now
	// that Subscribe push is the primary freshness signal.
	DefaultCacheAttrTTL = 5 * time.Minute
	// DefaultCacheDirTTL is the per-entry lifetime for directory
	// listing cache hits. Relaxed from 5s to 5min in Sub-spec D.
	DefaultCacheDirTTL = 5 * time.Minute
	// DefaultCacheNegativeTTL is the per-entry lifetime for negative
	// attribute cache entries. Relaxed from 2s to 30s in Sub-spec D;
	// Subscribe push invalidates on delete/rename so the TTL is the
	// last line of defence, not the first.
	DefaultCacheNegativeTTL = 30 * time.Second
	// DefaultCacheXAttrTTL is the per-entry lifetime for cached xattr-name
	// lists. Advisory/display-only (ACL enforcement is server-side), so it
	// mirrors the attr TTL and TTL+invalidation are the only freshness signals.
	DefaultCacheXAttrTTL = 5 * time.Minute
)

// defaultCachePath returns the XDG-default cache directory.
func defaultCachePath() string {
	return filepath.Join(xdg.CacheHome, "gmountie")
}

// CacheConfig governs the client-side cache. Sub-spec B introduced the
// in-memory tier; Sub-spec C adds persistence under Path with two
// independent byte caps. Disabled-by-default in B, enabled-by-default
// in C. Sub-spec D adds Subscribe-based push invalidation.
type CacheConfig struct {
	// Enabled gates whether the cache decorator is inserted at mount time.
	Enabled bool `mapstructure:"enabled"`
	// SubscribeEnabled gates the Subscribe push-invalidation path. When
	// true the client opens a persistent gRPC Subscribe stream; the
	// validity tracker starts unverified and flips to verified on the
	// first HEARTBEAT. When false the cache is marked globally verified
	// at construction and TTL alone drives eviction — equivalent to
	// Sub-spec C behaviour.
	SubscribeEnabled bool `mapstructure:"subscribe_enabled"`
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
	// Zero disables time-based expiry for this tier (entries live until
	// invalidated by Subscribe push or a mutating op).
	AttrTTL time.Duration `mapstructure:"attr_ttl"`
	// DirTTL is the per-entry lifetime for directory listing cache hits.
	// Zero disables time-based expiry for this tier.
	DirTTL time.Duration `mapstructure:"dir_ttl"`
	// NegativeTTL is the per-entry lifetime for negative attribute cache
	// entries (paths that returned ENOENT). Zero disables time-based
	// expiry for this tier.
	NegativeTTL time.Duration `mapstructure:"negative_ttl"`
	// XAttrTTL is the per-entry lifetime for cached xattr-name lists. Zero
	// disables time-based expiry for this tier.
	XAttrTTL time.Duration `mapstructure:"xattr_ttl"`
}

// defaultCacheConfig returns a CacheConfig seeded entirely from the
// DefaultCache* constants. Single source of the literal defaults, reused by
// the v==nil fast path and as the unmarshal target so it can never drift from
// the SetDefault block (kept only for --effective reflection) below.
func defaultCacheConfig() *CacheConfig {
	return &CacheConfig{
		Enabled:          DefaultCacheEnabled,
		SubscribeEnabled: DefaultCacheSubscribeEnabled,
		Path:             defaultCachePath(),
		MemoryMaxBytes:   DefaultCacheMemoryMaxBytes,
		DiskMaxBytes:     DefaultCacheDiskMaxBytes,
		ChunkSizeBytes:   DefaultCacheChunkSizeBytes,
		AttrTTL:          DefaultCacheAttrTTL,
		DirTTL:           DefaultCacheDirTTL,
		NegativeTTL:      DefaultCacheNegativeTTL,
		XAttrTTL:         DefaultCacheXAttrTTL,
	}
}

// NewCacheConfig parses a CacheConfig from a viper sub-tree. A nil v
// yields defaults; an empty sub-tree yields defaults; explicit values
// override.
func NewCacheConfig(v *viper.Viper) (*CacheConfig, error) {
	cfg := defaultCacheConfig()
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("enabled", DefaultCacheEnabled)
	v.SetDefault("subscribe_enabled", DefaultCacheSubscribeEnabled)
	v.SetDefault("path", defaultCachePath())
	v.SetDefault("memory_max_bytes", DefaultCacheMemoryMaxBytes)
	v.SetDefault("disk_max_bytes", DefaultCacheDiskMaxBytes)
	v.SetDefault("chunk_size_bytes", DefaultCacheChunkSizeBytes)
	v.SetDefault("attr_ttl", DefaultCacheAttrTTL)
	v.SetDefault("dir_ttl", DefaultCacheDirTTL)
	v.SetDefault("negative_ttl", DefaultCacheNegativeTTL)
	v.SetDefault("xattr_ttl", DefaultCacheXAttrTTL)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, err
	}
	return cfg, nil
}
