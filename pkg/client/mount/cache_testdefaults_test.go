package mount

import "gmountie/pkg/client/config"

// defaultTestCacheConfig returns a disabled CacheConfig populated with
// the Phase 4 Sub-spec B defaults. Mount-layer tests use this so the
// constructor signature matches production without exercising the cache
// path (Enabled=false keeps the wrap a no-op).
func defaultTestCacheConfig() config.CacheConfig {
	return config.CacheConfig{
		Enabled:        false,
		MaxSizeBytes:   config.DefaultCacheMaxSizeBytes,
		ChunkSizeBytes: config.DefaultCacheChunkSizeBytes,
		AttrTTL:        config.DefaultCacheAttrTTL,
		DirTTL:         config.DefaultCacheDirTTL,
		NegativeTTL:    config.DefaultCacheNegativeTTL,
	}
}
