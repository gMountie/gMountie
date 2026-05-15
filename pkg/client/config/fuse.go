package config

import (
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

const (
	// DefaultFUSEMaxWriteBytes is the default ceiling for FUSE MaxWrite.
	// go-fuse sets the kernel's max_read to the same value, so this knob
	// drives both directions of FUSE-kernel transfer size. 1 MiB matches
	// the server's default FrameSizeBytes; the version handshake caps
	// this down further when the server advertises a smaller frame.
	DefaultFUSEMaxWriteBytes = 1 << 20
	// DefaultFUSEMaxBackground is the upper bound on async background
	// requests in flight from the kernel. go-fuse's library default is
	// 12; 64 gives the streaming Read/Write path room to overlap.
	DefaultFUSEMaxBackground = 64
	// DefaultFUSEWritebackCache leaves the kernel writeback cache off
	// pending Phase 4's cache layer. Wired through ExtraCapabilities
	// (CAP_WRITEBACK_CACHE) at mount time when true.
	DefaultFUSEWritebackCache = false
)

// FUSEConfig holds the FUSE-kernel-side tuning knobs surfaced through
// gmountie. Values are passed straight to fuse.MountOptions when building
// the mount; max_write_bytes is additionally capped at the server's
// advertised frame size during the version handshake.
//
// go-fuse v2.10.1 does not expose a separate MaxRead field — the kernel's
// max_read is set equal to MaxWrite by the library, so one knob covers
// both directions. There is also no CongestionThreshold field in v2.10.1.
type FUSEConfig struct {
	// MaxWriteBytes bounds each FUSE WRITE (and effectively each READ —
	// see note above) the kernel issues. Pinned to [4 KiB, 16 MiB] so
	// values below a page size or above gRPC's default ceiling are
	// rejected up-front.
	MaxWriteBytes int `validate:"min=4096,max=16777216" mapstructure:"max_write_bytes"`
	// MaxBackground caps async background requests in flight. Pinned to
	// [1, 1024]; the upper bound is a sanity ceiling, not a tuned value.
	MaxBackground int `validate:"min=1,max=1024" mapstructure:"max_background"`
	// WritebackCache toggles the kernel's writeback page cache for the
	// mount. Off by default — the read/write path is still synchronous
	// pending Phase 4's cache layer.
	WritebackCache bool `mapstructure:"writeback_cache"`
}

// NewFUSEConfig parses a FUSEConfig from a viper sub-tree. A nil v
// yields defaults; an empty sub-tree yields defaults; explicit values
// override.
func NewFUSEConfig(v *viper.Viper) (*FUSEConfig, error) {
	cfg := &FUSEConfig{
		MaxWriteBytes:  DefaultFUSEMaxWriteBytes,
		MaxBackground:  DefaultFUSEMaxBackground,
		WritebackCache: DefaultFUSEWritebackCache,
	}
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("max_write_bytes", DefaultFUSEMaxWriteBytes)
	v.SetDefault("max_background", DefaultFUSEMaxBackground)
	v.SetDefault("writeback_cache", DefaultFUSEWritebackCache)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, err
	}
	return cfg, nil
}
