package mount

import "gmountie/pkg/client/config"

// defaultTestCacheConfig returns a disabled CacheConfig for use in
// mount-layer tests. Enabled=false keeps the cache wrap a no-op so
// tests exercise the raw gRPC backend, not the cache decorator.
func defaultTestCacheConfig() config.CacheConfig {
	return config.CacheConfig{
		Enabled:        false,
		MemoryMaxBytes: config.DefaultCacheMemoryMaxBytes,
		DiskMaxBytes:   config.DefaultCacheDiskMaxBytes,
		ChunkSizeBytes: config.DefaultCacheChunkSizeBytes,
		AttrTTL:        config.DefaultCacheAttrTTL,
		DirTTL:         config.DefaultCacheDirTTL,
		NegativeTTL:    config.DefaultCacheNegativeTTL,
	}
}
