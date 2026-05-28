package config

import (
	"fmt"
	serverConfig "gmountie/pkg/server/config"

	"github.com/spf13/viper"
)

// BasicAuthConfigUser holds the cleartext credentials the client sends to
// the server over TLS. This is deliberately separate from the server-side
// BasicAuthConfigUser (which stores an argon2id hash, not a plaintext password).
type BasicAuthConfigUser struct {
	Username string `validate:"required"`
	Password string `validate:"required"`
}

// BasicAuthConfig is a struct that holds the configuration for the basic auth user
type BasicAuthConfig struct {
	Type             serverConfig.AuthConfigType `validate:"required"`
	BasicAuthConfigUser `yaml:",inline"`
}

func (b BasicAuthConfig) GetType() serverConfig.AuthConfigType {
	return serverConfig.AuthConfigTypeBasic
}

// NewBasicAuthConfig creates a new BasicAuthConfig with defaults
func NewBasicAuthConfig(v *viper.Viper) (*BasicAuthConfig, error) {
	var user BasicAuthConfigUser
	if err := v.Unmarshal(&user); err != nil {
		return nil, err
	}

	return &BasicAuthConfig{
		Type:                serverConfig.AuthConfigTypeBasic,
		BasicAuthConfigUser: user,
	}, nil
}

// NewAuthFromConfig creates a new AuthConfig from a viper config
func NewAuthFromConfig(v *viper.Viper) (serverConfig.AuthConfig, error) {
	var auth serverConfig.AuthConfig
	var err error
	switch v.GetString("type") {
	case "basic":
		auth, err = NewBasicAuthConfig(v)
	default:
		return nil, fmt.Errorf("invalid auth type: %q (only 'basic' is supported)", v.GetString("type"))
	}

	if err != nil {
		return nil, err
	}
	return auth, nil
}
