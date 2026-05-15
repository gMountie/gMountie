package config

import (
	"fmt"
	"strings"

	"gmountie/pkg/common/config"
	"gmountie/pkg/utils/log"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

const (
	EnvironmentPrefix = "GMOUNTIE"
)

// Config is a struct that holds the configuration for the server
type Config struct {
	// Server is the server configuration
	Server *ServerConfig `validate:"required"`

	// Auth is the auth configuration
	Auth AuthConfig `validate:"required"`

	// Volumes is the volume configuration
	Volumes []*VolumeConfig `validate:"required,dive"`

	// Log is the optional logger configuration. Nil keeps the
	// init-time auto-detected defaults.
	Log *log.LogConfig
}

func LoadConfigFromString(cfg string) (*Config, error) {
	return config.LoadConfigFromString(cfg, ParseConfig)
}

func ParseConfig(v *viper.Viper) (*Config, error) {
	var result Config

	// Enable environment variable overrides. `GMOUNTIE_SERVER_PORT` →
	// `server.port`, etc. The env key replacer maps `_` to `.` so nested
	// keys can be reached. AutomaticEnv alone doesn't propagate through
	// Sub(...), so we explicitly bind the nested keys we want overridable
	// and read them from the parent viper directly.
	v.SetEnvPrefix(EnvironmentPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	for _, key := range []string{
		"server.address",
		"server.port",
		"server.metrics",
		"server.metrics_addr",
		"server.frame_size_bytes",
		"server.compound_max_parallel",
		"auth.type",
	} {
		_ = v.BindEnv(key)
	}

	// Parse the server configuration — read directly from the parent viper
	// to honour env-var overrides (see comment above).
	v.SetDefault("server.address", DefaultAddress)
	v.SetDefault("server.port", DefaultPort)
	v.SetDefault("server.metrics", true)
	v.SetDefault("server.metrics_addr", DefaultMetricsAddr)
	v.SetDefault("server.frame_size_bytes", DefaultFrameSizeBytes)
	v.SetDefault("server.compound_max_parallel", DefaultCompoundMaxParallel)
	result.Server = &ServerConfig{
		Address:             v.GetString("server.address"),
		Port:                v.GetUint("server.port"),
		Metrics:             v.GetBool("server.metrics"),
		MetricsAddr:         v.GetString("server.metrics_addr"),
		FrameSizeBytes:      v.GetInt("server.frame_size_bytes"),
		CompoundMaxParallel: v.GetInt("server.compound_max_parallel"),
	}

	// Parse the auth configuration.
	auth, err := NewFromConfig(v.Sub("auth"))
	if err != nil {
		return nil, err
	}
	result.Auth = auth

	// Parse the volume configuration.
	volumes := make([]*VolumeConfig, 0)
	for sub, i := v.Sub("volumes.0"), 0; sub != nil; sub = v.Sub(fmt.Sprintf("volumes.%d", i)) {
		volumes = append(volumes, NewVolumeConfig(sub))
		i++
	}
	result.Volumes = volumes

	// Parse the log configuration (env + defaults).
	v.SetDefault("log.format", "")
	v.SetDefault("log.level", "")
	_ = v.BindEnv("log.format")
	_ = v.BindEnv("log.level")
	result.Log = &log.LogConfig{
		Format: v.GetString("log.format"),
		Level:  v.GetString("log.level"),
	}

	// Validate.
	validate := validator.New()
	err = validate.Struct(result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
