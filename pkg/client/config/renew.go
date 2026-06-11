package config

import (
	"fmt"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// DefaultRenewBefore is how long before cert expiry renewal starts — the
// ACME-conventional final third of a 24h cert.
const DefaultRenewBefore = 8 * time.Hour

// RenewConfig configures the opt-in client-certificate refresher: the client
// exchanges a bearer token + CSR at Endpoint for a short-lived in-memory
// client cert and renews it Before expiry. An absent block / empty Endpoint
// disables renewal entirely — the client behaves exactly as without it.
type RenewConfig struct {
	// Endpoint is the base HTTPS URL of the token→certificate exchange;
	// the client calls {Endpoint}/profile and {Endpoint}/renew.
	Endpoint string `mapstructure:"endpoint"`
	// Token is the inline bearer token. TokenFile wins when both are set.
	Token string `mapstructure:"token"`
	// TokenFile is a path whose (trimmed) contents are the bearer token,
	// re-read on every exchange so the token can rotate without a restart.
	TokenFile string `mapstructure:"token_file"`
	// Before is the renewal lead time before cert expiry.
	Before time.Duration `mapstructure:"before"`
}

// NewRenewConfig builds a RenewConfig from the "renew" sub-viper (nil-safe).
func NewRenewConfig(v *viper.Viper) (*RenewConfig, error) {
	cfg := &RenewConfig{Before: DefaultRenewBefore}
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("before", DefaultRenewBefore)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, fmt.Errorf("parse renew config: %w", err)
	}
	if cfg.Enabled() {
		if cfg.Token == "" && cfg.TokenFile == "" {
			return nil, fmt.Errorf("renew: endpoint is set but no token or token_file")
		}
		if cfg.Before <= 0 {
			return nil, fmt.Errorf("renew: before must be positive, got %s", cfg.Before)
		}
	}
	return cfg, nil
}

// Enabled reports whether the refresher is configured. Nil-safe.
func (c *RenewConfig) Enabled() bool { return c != nil && c.Endpoint != "" }
