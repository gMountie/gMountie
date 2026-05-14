package config

import (
	"errors"
	"fmt"

	"github.com/spf13/viper"
)

type AuthConfigType string

const (
	AuthConfigTypeNone  AuthConfigType = "none"
	AuthConfigTypeBasic AuthConfigType = "basic"
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
	case "none":
		auth = NewNoneAuthConfig(v)
	case "basic":
		auth, err = NewBasicAuthConfig(v)
	default:
		return nil, fmt.Errorf("invalid auth type: %s", v.GetString("type"))
	}

	if err != nil {
		return nil, err
	}

	return auth, nil
}

// ------------- NoneAuthConfig -------------

// NoneAuthConfig is a struct that holds the configuration for the none auth
type NoneAuthConfig struct {
	AuthConfigBase
}

// NewNoneAuthConfig creates a new NoneAuthConfig with defaults
func NewNoneAuthConfig(v *viper.Viper) *NoneAuthConfig {
	return &NoneAuthConfig{
		AuthConfigBase: AuthConfigBase{
			Type: AuthConfigTypeNone,
		},
	}
}

// GetType returns the type of the auth configuration
func (n *NoneAuthConfig) GetType() AuthConfigType {
	return AuthConfigTypeNone
}

// ------------- BasicAuthConfig -------------

// BasicAuthConfigUser is a struct that holds the configuration for a basic auth user
type BasicAuthConfigUser struct {
	Username string `validate:"required"`
	Password string `validate:"required"`
}

// BasicAuthConfig is a struct that holds the configuration for the basic auth
type BasicAuthConfig struct {
	AuthConfigBase
	Users []BasicAuthConfigUser `validate:"required,dive"`
}

// NewBasicAuthConfig creates a new BasicAuthConfig with defaults
func NewBasicAuthConfig(v *viper.Viper) (*BasicAuthConfig, error) {
	var conf BasicAuthConfig
	conf.Type = AuthConfigTypeBasic
	err := v.Unmarshal(&conf)
	if err != nil {
		return nil, err
	}
	return &conf, nil
}

// GetType returns the type of the auth configuration
func (b *BasicAuthConfig) GetType() AuthConfigType {
	return AuthConfigTypeBasic
}
