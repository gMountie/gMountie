package config

import (
	"gmountie/pkg/common/config"
	serverConfig "gmountie/pkg/server/config"
	"gmountie/pkg/utils/log"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// Config is a struct that holds the configuration for the client
type Config struct {
	// Server is the server configuration
	Server *ServerConfig `validate:"required"`
	// Auth is the authentication configuration
	Auth serverConfig.AuthConfig `validate:"required"`
	// Mount is the mount configuration
	Mount MountConfig `yaml:"mount,omitempty"`
	// Rpc is the RPC configuration
	Rpc *RpcConfig `validate:"required" yaml:"rpc,omitempty"`
}

// String returns the string representation of the Config
func (c *Config) String() (string, error) {
	out, err := yaml.Marshal(c)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// Validate validates the Config
func (c *Config) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}

// Save saves the Config to a file
func (c *Config) Save(path string) error {
	content, err := c.String()
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Wrapf(err, "error opening file: %s", path)
	}
	defer func() {
		err = file.Close()
		if err != nil {
			log.Log.Error("error closing file", zap.Error(err))
		}
	}()

	_, err = file.WriteString(content)
	if err != nil {
		return err
	}
	return nil
}

// LoadConfigFromString loads a Config from a string
func LoadConfigFromString(cfg string) (*Config, error) {
	c, err := config.LoadConfigFromString(cfg, ParseConfig)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// ParseConfig parses a Config from a viper.Viper
func ParseConfig(v *viper.Viper) (*Config, error) {
	var result Config

	// Parse server config
	v.SetDefault("server", make(map[string]string))
	if cfg, err := NewServerConfig(v.Sub("server")); err == nil {
		result.Server = cfg
	} else {
		return nil, err
	}
	// Parse auth config
	v.SetDefault("auth", make(map[string]string))
	if cfg, err := NewAuthFromConfig(v.Sub("auth")); err == nil {
		result.Auth = cfg
	} else {
		return nil, err
	}
	// Parse mount config
	mount := v.Sub("mount")
	if mount != nil {
		if cfg, err := NewMountConfig(v.Sub("mount")); err == nil {
			result.Mount = cfg
		} else {
			return nil, err
		}
	}

	// Parse rpc config (defaults if absent)
	if cfg, err := NewRpcConfig(v.Sub("rpc")); err == nil {
		result.Rpc = cfg
	} else {
		return nil, err
	}

	if err := result.Validate(); err != nil {
		return nil, err
	}
	return &result, nil
}
