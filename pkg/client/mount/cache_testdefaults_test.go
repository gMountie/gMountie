package mount

import "go.gmountie.dev/gmountie/pkg/client/config"

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

// defaultTestWALConfig returns a disabled WALConfig for use in mount-layer
// tests. Enabled=false keeps the WAL/delegation machinery out of the compose
// stack (the production default), so cache-enabled tests exercise the
// synchronous cache→transport path without overlay/delegation/flusher.
func defaultTestWALConfig() config.WALConfig {
	return config.WALConfig{Enabled: false}
}

// walEnabledTestConfig returns an enabled WALConfig for tests that need to
// exercise the WAL/delegation construction path.
func walEnabledTestConfig() config.WALConfig {
	return config.WALConfig{Enabled: true}
}

// defaultTestFUSEConfig returns a FUSEConfig populated with all defaults
// for use in mount-layer tests. Constructing it explicitly (rather than
// relying on zero values) ensures new fields are exercised at their
// documented defaults rather than silently zero.
func defaultTestFUSEConfig() *config.FUSEConfig {
	return &config.FUSEConfig{
		MaxWriteBytes:  config.DefaultFUSEMaxWriteBytes,
		MaxBackground:  config.DefaultFUSEMaxBackground,
		WritebackCache: config.DefaultFUSEWritebackCache,
		AttrTimeout:    config.DefaultFUSEAttrTimeout,
		EntryTimeout:   config.DefaultFUSEEntryTimeout,
	}
}
