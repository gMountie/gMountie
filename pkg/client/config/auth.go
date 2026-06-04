package config

import (
	"fmt"

	serverConfig "go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/spf13/viper"
)

// BasicAuthConfigUser holds the cleartext credentials the client sends to
// the server over TLS. This is deliberately separate from the server-side
// BasicAuthConfigUser (which stores an argon2id hash, not a plaintext password).
//
// Password is omitempty because the plaintext secret is resolved at mount time
// by resolveAuth (password_command / password_file / env / interactive prompt) —
// not at config-parse time. A config that omits the inline password is valid
// here; resolveAuth errors if no source produces a password at mount time.
//
// PasswordCommand and PasswordFile are alternative password sources resolved at
// mount time by resolveConfiguredPassword; they are not secrets (a shell command
// or a file path) and render verbatim in config show --effective.
type BasicAuthConfigUser struct {
	Username        string `mapstructure:"username" validate:"required"`
	Password        string `mapstructure:"password" validate:"omitempty"`
	PasswordCommand string `mapstructure:"password_command" validate:"omitempty"`
	PasswordFile    string `mapstructure:"password_file" validate:"omitempty"`
}

// BasicAuthConfig is a struct that holds the configuration for the basic auth user
type BasicAuthConfig struct {
	Type                serverConfig.AuthConfigType `validate:"required"`
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
	case string(serverConfig.AuthConfigTypeBasic):
		auth, err = NewBasicAuthConfig(v)
	case string(serverConfig.AuthConfigTypeMTLS):
		// mTLS needs no username/password: the verified client certificate is
		// the identity. AuthConfigBase satisfies the AuthConfig interface and
		// carries no required fields, so the validator passes. The grpc factory
		// skips per-RPC basic-auth for any non-BasicAuthConfig auth.
		auth = &serverConfig.AuthConfigBase{Type: serverConfig.AuthConfigTypeMTLS}
	default:
		return nil, fmt.Errorf("invalid auth type: %q (only 'basic' and 'mtls' are supported)", v.GetString("type"))
	}

	if err != nil {
		return nil, err
	}
	return auth, nil
}
