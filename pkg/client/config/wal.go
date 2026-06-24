package config

import (
	"github.com/spf13/viper"
)

const (
	// DefaultWALEnabled gates the client-side WAL / delegation feature
	// (deferred writes + in-memory overlay + flush + server delegation).
	// Defaults to FALSE: the WAL/delegation machinery is not yet
	// production-stable, so releases default to the proven synchronous
	// write path and WAL is opt-in for testing. When false the client
	// never builds the overlay/coordinator/flusher and never requests
	// delegations (so the server never delegates).
	DefaultWALEnabled = false
)

// WALConfig governs the client-side write-ahead-log / delegation feature.
// WAL rides on the cache (it sits outer of the cache layer); it is only
// active when both the cache and this flag are enabled.
type WALConfig struct {
	// Enabled gates whether the WAL/delegation machinery is built at mount
	// time. Default false: writes go straight through the cache to the
	// transport synchronously and no delegations are requested.
	Enabled bool `mapstructure:"enabled"`
}

// defaultWALConfig returns a WALConfig seeded from the DefaultWAL* constants.
// Single source of the literal defaults, reused by the v==nil fast path and as
// the unmarshal target so it can never drift from the SetDefault block below.
func defaultWALConfig() *WALConfig {
	return &WALConfig{
		Enabled: DefaultWALEnabled,
	}
}

// NewWALConfig parses a WALConfig from a viper sub-tree. A nil v yields
// defaults; an empty sub-tree yields defaults; explicit values override.
func NewWALConfig(v *viper.Viper) (*WALConfig, error) {
	cfg := defaultWALConfig()
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("enabled", DefaultWALEnabled)
	if err := v.UnmarshalExact(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
