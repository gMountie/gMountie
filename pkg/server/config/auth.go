package config

import (
	"errors"
	"fmt"

	"go.gmountie.dev/gmountie/pkg/common/passhash"

	"github.com/spf13/viper"
)

type AuthConfigType string

const (
	AuthConfigTypeBasic AuthConfigType = "basic"
	AuthConfigTypeMTLS  AuthConfigType = "mtls"
)

type AuthConfig interface {
	// GetType returns the type of the auth configuration
	GetType() AuthConfigType
}

// AuthConfigBase is a struct that holds the configuration for the auth
type AuthConfigBase struct {
	Type AuthConfigType
}

func (a *AuthConfigBase) GetType() AuthConfigType {
	return a.Type
}

// NewFromConfig creates a new AuthConfig from a viper config
func NewFromConfig(v *viper.Viper) (AuthConfig, error) {
	if v == nil {
		return nil, errors.New("auth: missing 'auth' section in config")
	}
	var auth AuthConfig
	var err error
	switch v.GetString("type") {
	case "basic":
		auth, err = NewBasicAuthConfig(v)
	case "mtls":
		auth, err = NewMTLSAuthConfig(v)
	default:
		return nil, fmt.Errorf("invalid auth type: %q (supported: basic, mtls)", v.GetString("type"))
	}

	if err != nil {
		return nil, err
	}

	return auth, nil
}

// ------------- BasicAuthConfig -------------

// BasicAuthConfigUser is a struct that holds the configuration for a basic auth user
type BasicAuthConfigUser struct {
	Username string `validate:"required"`
	// PasswordHash is required for basic auth; omitted (or empty) for mtls users
	// (where the verified client cert is the credential). The IsHashed check in
	// NewBasicAuthConfig enforces presence for type:basic only.
	PasswordHash string `mapstructure:"password_hash"`
	// Volumes is the explicit list of volume names this user may access.
	// Empty/unset means use the default policy (default_allow). Explicit []
	// means no volume access regardless of default_allow.
	Volumes []string `mapstructure:"volumes"`
}

// BasicAuthConfig is a struct that holds the configuration for the basic auth
type BasicAuthConfig struct {
	AuthConfigBase
	Users []BasicAuthConfigUser `validate:"required,dive"`
	// DefaultAllow controls access for principals that have no explicit volumes
	// list. nil (unset) is treated as true for backwards compatibility.
	DefaultAllow *bool `mapstructure:"default_allow"`
}

// DefaultAllowOrTrue returns the effective default_allow value.
// nil (unset in config) is treated as true for backwards compatibility.
func (c *BasicAuthConfig) DefaultAllowOrTrue() bool {
	return c.DefaultAllow == nil || *c.DefaultAllow
}

// NewBasicAuthConfig creates a new BasicAuthConfig with defaults
func NewBasicAuthConfig(v *viper.Viper) (*BasicAuthConfig, error) {
	var conf BasicAuthConfig
	conf.Type = AuthConfigTypeBasic
	err := v.Unmarshal(&conf)
	if err != nil {
		return nil, err
	}
	for i, u := range conf.Users {
		if !passhash.IsHashed(u.PasswordHash) {
			return nil, fmt.Errorf("auth.users[%d].password_hash must be a $argon2id$ PHC string; run 'gmountie genpass' and paste the output", i)
		}
	}
	return &conf, nil
}

// GetType returns the type of the auth configuration.
// Delegates to the embedded AuthConfigBase so the value set by the factory
// constructor is returned faithfully (basic or mtls).
func (b *BasicAuthConfig) GetType() AuthConfigType {
	return b.Type
}

// NewMTLSAuthConfig creates a BasicAuthConfig typed as mtls.
// Users carry Username + Volumes for the volume ACL; PasswordHash is unused
// and must not be present (the verified client cert is the credential).
func NewMTLSAuthConfig(v *viper.Viper) (*BasicAuthConfig, error) {
	var conf BasicAuthConfig
	conf.Type = AuthConfigTypeMTLS
	err := v.Unmarshal(&conf)
	if err != nil {
		return nil, err
	}
	// Enforce: mtls users must not carry password_hash (the cert is the cred).
	for i, u := range conf.Users {
		if u.PasswordHash != "" {
			return nil, fmt.Errorf(
				"auth.users[%d].password_hash must be empty for auth.type: mtls "+
					"(the verified client certificate is the credential)",
				i,
			)
		}
	}
	return &conf, nil
}
