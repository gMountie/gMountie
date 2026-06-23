package config

import (
	"os"
	"strings"

	"go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/pkg/utils/log"

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
	Auth config.AuthConfig `validate:"required"`
	// Mount is the mount configuration
	Mount MountConfig `yaml:"mount,omitempty"`
	// Rpc is the RPC configuration
	Rpc *RpcConfig `validate:"required" yaml:"rpc,omitempty"`
	// FUSE is the FUSE-kernel-side tuning configuration (mount options).
	FUSE *FUSEConfig `validate:"required" yaml:"fuse,omitempty"`
	// Cache is the client-side cache configuration. Sub-spec B of Phase 4
	// adds an in-memory cache layer; Sub-spec C adds persistence (Path,
	// DiskMaxBytes) and flips the default to enabled.
	Cache *CacheConfig `validate:"required" yaml:"cache,omitempty"`
	// Renew is the optional client-certificate refresher configuration.
	// Absent → disabled (static certs, exactly as before).
	Renew *RenewConfig `yaml:"renew,omitempty"`
	// Log is the optional logger configuration. Nil keeps the
	// init-time auto-detected defaults.
	Log *log.LogConfig `yaml:"log,omitempty"`
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
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
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

// mirrorEnvToSub binds each "section.key" in the parent viper so
// AutomaticEnv can resolve them, then copies any set values into the
// sub-viper so nested unmarshal picks them up. Returns a non-nil sub-viper
// (creates a fresh one if the YAML section is absent).
//
// This is required because viper.Sub() returns a snapshot of the in-file
// values and does not inherit env-var overrides from the parent.
func mirrorEnvToSub(parent *viper.Viper, section string, keys []string) *viper.Viper {
	for _, k := range keys {
		_ = parent.BindEnv(section + "." + k)
	}
	sub := parent.Sub(section)
	if sub == nil {
		sub = viper.New()
	}
	for _, k := range keys {
		if parent.IsSet(section + "." + k) {
			sub.Set(k, parent.Get(section+"."+k))
		}
	}
	return sub
}

// ParseConfig parses a Config from a viper.Viper
func ParseConfig(v *viper.Viper) (*Config, error) {
	var result Config

	// Enable environment variable overrides. `GMOUNTIE_LOG_LEVEL` →
	// `log.level`, etc. AutomaticEnv alone doesn't propagate through
	// Sub(...), so nested keys we want overridable are bound explicitly
	// and read from the parent viper directly.
	v.SetEnvPrefix(config.EnvironmentPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Parse server config. mirrorEnvToSub binds env vars and copies any
	// overridden values into the sub-tree so UnmarshalExact sees them.
	serverSub := mirrorEnvToSub(v, "server", []string{
		"tls.verify",
		"tls.ca_file",
		"tls.expected_fingerprint",
		"tls.server_name",
		"tls.cert_file",
		"tls.key_file",
		"tls.ca_pem",
		"tls.cert_pem",
		"tls.key_pem",
	})
	if cfg, err := NewServerConfig(serverSub); err == nil {
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

	// Parse fuse config (defaults if absent). mirrorEnvToSub wires env-var
	// overrides (e.g. GMOUNTIE_FUSE_ATTR_TIMEOUT) into the sub-tree so
	// AutomaticEnv propagates through viper.Sub as it does for cache/server.
	fuseSub := mirrorEnvToSub(v, "fuse", []string{
		"max_write_bytes",
		"max_background",
		"writeback_cache",
		"direct_io",
		"attr_timeout",
		"entry_timeout",
		"handle_kill_priv",
		"auto_xattr",
	})
	if cfg, err := NewFUSEConfig(fuseSub); err == nil {
		result.FUSE = cfg
	} else {
		return nil, err
	}

	// Parse cache config. mirrorEnvToSub wires env-var overrides into the
	// sub-tree (AutomaticEnv doesn't propagate through viper.Sub).
	cacheSub := mirrorEnvToSub(v, "cache", []string{
		"enabled",
		"path",
		"memory_max_bytes",
		"disk_max_bytes",
		"chunk_size_bytes",
		"attr_ttl",
		"dir_ttl",
		"negative_ttl",
	})
	if cfg, err := NewCacheConfig(cacheSub); err == nil {
		result.Cache = cfg
	} else {
		return nil, err
	}

	// Parse renew config (optional; absent block yields disabled defaults).
	renewSub := mirrorEnvToSub(v, "renew", []string{
		"endpoint", "token", "token_file", "before",
	})
	if cfg, err := NewRenewConfig(renewSub); err == nil {
		result.Renew = cfg
	} else {
		return nil, err
	}

	// Parse log config (optional; absent block keeps zero-value defaults).
	v.SetDefault("log.format", "")
	v.SetDefault("log.level", "")
	_ = v.BindEnv("log.format")
	_ = v.BindEnv("log.level")
	result.Log = &log.LogConfig{
		Format: v.GetString("log.format"),
		Level:  v.GetString("log.level"),
	}

	if err := result.Validate(); err != nil {
		return nil, config.FriendlyValidationError(err)
	}
	return &result, nil
}
