package config

import (
	"fmt"
	"strings"

	"go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/pkg/utils/log"

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

	// ConfigPath is the file this config was loaded from, set by the loader
	// (serve.go / ReloadFromFile). Empty when built from a string/defaults.
	// Used by /ops/acl/reload to re-read the file. Not parsed from YAML.
	ConfigPath string `mapstructure:"-"`
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
		"server.max_message_bytes",
		"server.keepalive.time",
		"server.keepalive.timeout",
		"server.keepalive.min_time",
		"server.keepalive.permit_without_stream",
		"server.subscribe_buffer_size",
		"server.subscribe_heartbeat_interval",
		"server.tls.cert_file",
		"server.tls.key_file",
		"server.tls.client_ca_file",
		"server.tls.min_version",
		"server.tls.disabled",
		"server.ops.addr",
		"server.ops.auth.type",
		"server.grpc.reflection",
		"server.grpc.limits.max_recv_message_size",
		"server.grpc.limits.max_concurrent_streams",
		"server.grpc.limits.max_connection_idle",
		"server.grpc.limits.max_connection_age",
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
	v.SetDefault("server.max_message_bytes", DefaultMaxMessageBytes)
	v.SetDefault("server.keepalive.time", DefaultKeepaliveTime)
	v.SetDefault("server.keepalive.timeout", DefaultKeepaliveTimeout)
	v.SetDefault("server.keepalive.min_time", DefaultKeepaliveMinTime)
	v.SetDefault("server.keepalive.permit_without_stream", DefaultKeepalivePermitWithoutStream)
	v.SetDefault("server.subscribe_buffer_size", DefaultServerSubscribeBufferSize)
	v.SetDefault("server.subscribe_heartbeat_interval", DefaultServerSubscribeHeartbeatInterval)
	v.SetDefault("server.tls.min_version", "1.3")
	v.SetDefault("server.ops.addr", "127.0.0.1:9090")
	v.SetDefault("server.ops.auth.type", "none")
	v.SetDefault("server.grpc.reflection", false)
	v.SetDefault("server.grpc.limits.max_recv_message_size", DefaultMaxMessageBytes)
	v.SetDefault("server.grpc.limits.max_concurrent_streams", uint32(256))
	v.SetDefault("server.grpc.limits.max_connection_idle", "5m")
	v.SetDefault("server.grpc.limits.max_connection_age", 0)
	result.Server = &ServerConfig{
		Address:             v.GetString("server.address"),
		Port:                v.GetUint("server.port"),
		Metrics:             v.GetBool("server.metrics"),
		MetricsAddr:         v.GetString("server.metrics_addr"),
		Pprof:               v.GetBool("server.pprof"),
		FrameSizeBytes:      v.GetInt("server.frame_size_bytes"),
		CompoundMaxParallel: v.GetInt("server.compound_max_parallel"),
		MaxMessageBytes:     v.GetInt("server.max_message_bytes"),
		Keepalive: ServerKeepaliveConfig{
			Time:                v.GetDuration("server.keepalive.time"),
			Timeout:             v.GetDuration("server.keepalive.timeout"),
			MinTime:             v.GetDuration("server.keepalive.min_time"),
			PermitWithoutStream: v.GetBool("server.keepalive.permit_without_stream"),
		},
		SubscribeBufferSize:        v.GetInt("server.subscribe_buffer_size"),
		SubscribeHeartbeatInterval: v.GetDuration("server.subscribe_heartbeat_interval"),
		TLS: TLSConfig{
			CertFile:     v.GetString("server.tls.cert_file"),
			KeyFile:      v.GetString("server.tls.key_file"),
			ClientCAFile: v.GetString("server.tls.client_ca_file"),
			MinVersion:   v.GetString("server.tls.min_version"),
			Disabled:     v.GetBool("server.tls.disabled"),
		},
		Ops: parseOpsConfig(v),
		GRPC: GRPCConfig{
			Reflection: v.GetBool("server.grpc.reflection"),
			Limits: LimitsConfig{
				MaxRecvMessageSize:   v.GetInt("server.grpc.limits.max_recv_message_size"),
				MaxConcurrentStreams: uint32(v.GetUint("server.grpc.limits.max_concurrent_streams")),
				MaxConnectionIdle:    v.GetDuration("server.grpc.limits.max_connection_idle"),
				MaxConnectionAge:     v.GetDuration("server.grpc.limits.max_connection_age"),
			},
		},
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

// parseOpsConfig reads the server.ops.* keys from the parent viper and builds
// an OpsConfig. Users are decoded via mapstructure through a viper.Sub so that
// array-of-struct values unmarshal correctly.
func parseOpsConfig(v *viper.Viper) OpsConfig {
	cfg := OpsConfig{
		Addr: v.GetString("server.ops.addr"),
		Auth: OpsAuthConfig{
			Type: v.GetString("server.ops.auth.type"),
		},
		TLS: OpsTLSConfig{
			CertFile:     v.GetString("server.ops.tls.cert_file"),
			KeyFile:      v.GetString("server.ops.tls.key_file"),
			ClientCAFile: v.GetString("server.ops.tls.client_ca_file"),
		},
	}
	// Unmarshal the users slice (only present when auth.type: basic).
	if sub := v.Sub("server.ops.auth"); sub != nil {
		var users []BasicAuthConfigUser
		if err := sub.UnmarshalKey("users", &users); err == nil {
			cfg.Auth.Users = users
		}
	}
	return cfg
}
