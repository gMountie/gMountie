package config

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

// Config is a struct that holds the configuration for the server
type Config struct {
	// Server is the server configuration
	Server *ServerConfig `validate:"required"`

	// Auth is the auth configuration
	Auth config.AuthConfig `validate:"required"`

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
	v.SetEnvPrefix(config.EnvironmentPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	for _, key := range []string{
		"server.address",
		"server.port",
		"server.metrics",
		"server.plain_metrics_addr",
		"server.frame_size_bytes",
		"server.keepalive.time",
		"server.keepalive.timeout",
		"server.keepalive.min_time",
		"server.keepalive.permit_without_stream",
		"server.subscribe_buffer_size",
		"server.subscribe_heartbeat_interval",
		"server.session.grace_period",
		"server.session.idempotency_cache_size",
		"server.identity.executor_workers",
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
	server, err := NewServerConfig(v)
	if err != nil {
		return nil, err
	}
	result.Server = server

	// Parse the auth configuration.
	auth, err := NewFromConfig(v.Sub("auth"))
	if err != nil {
		return nil, err
	}
	result.Auth = auth

	// Parse the volume configuration. A bad mapping block fails the whole
	// config load (fail-closed: identity mapping must never part-decode).
	volumes := make([]*VolumeConfig, 0)
	for sub, i := v.Sub("volumes.0"), 0; sub != nil; sub = v.Sub(fmt.Sprintf("volumes.%d", i)) {
		vc, err := NewVolumeConfig(sub)
		if err != nil {
			return nil, errors.Wrapf(err, "volumes[%d]", i)
		}
		volumes = append(volumes, vc)
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

	// Validate. Surface validation failures as human-readable config errors
	// rather than raw validator.ValidationErrors noise.
	validate := validator.New()
	if err = validate.Struct(result); err != nil {
		return nil, config.FriendlyValidationError(err)
	}

	return &result, nil
}

// defaultServerConfig is the single source of truth for ServerConfig defaults,
// seeded from the Default* constants (mirrors client config.NewRpcConfig). Both
// the v==nil path and the SetDefault seeding below derive from it, so the two
// can never drift.
func defaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Address: DefaultAddress,
		Port:    config.DefaultPort,
		Metrics: DefaultMetrics,
		Keepalive: ServerKeepaliveConfig{
			Time:                DefaultKeepaliveTime,
			Timeout:             DefaultKeepaliveTimeout,
			MinTime:             DefaultKeepaliveMinTime,
			PermitWithoutStream: DefaultKeepalivePermitWithoutStream,
		},
		Session: SessionConfig{
			GracePeriod:          DefaultSessionGracePeriod,
			IdempotencyCacheSize: DefaultSessionIdempotencyCacheSize,
		},
		Identity: IdentityConfig{
			ExecutorWorkers: DefaultIdentityExecutorWorkers,
		},
		FrameSizeBytes:             DefaultFrameSizeBytes,
		SubscribeBufferSize:        DefaultServerSubscribeBufferSize,
		SubscribeHeartbeatInterval: DefaultServerSubscribeHeartbeatInterval,
		TLS: TLSConfig{
			MinVersion: DefaultTLSMinVersion,
		},
		Ops: OpsConfig{
			Addr: DefaultOpsAddr,
			Auth: OpsAuthConfig{Type: DefaultOpsAuthType},
		},
		GRPC: GRPCConfig{
			Reflection: DefaultReflection,
			Limits: LimitsConfig{
				MaxRecvMessageSize:   DefaultMaxMessageBytes,
				MaxConcurrentStreams: DefaultMaxConcurrentStreams,
				MaxConnectionIdle:    DefaultMaxConnectionIdle,
				MaxConnectionAge:     DefaultMaxConnectionAge,
			},
		},
	}
}

// NewServerConfig builds a ServerConfig. A nil v yields the constant defaults
// (the defaults-in-constructor source of truth, mirroring NewRpcConfig). A
// non-nil v is read directly off the PARENT viper with the "server." prefix —
// NOT via v.Sub — because AutomaticEnv overrides don't propagate through Sub
// (see the env-bind comment in ParseConfig); explicit values override the
// seeded defaults.
func NewServerConfig(v *viper.Viper) (*ServerConfig, error) {
	cfg := defaultServerConfig()
	if v == nil {
		return cfg, nil
	}

	v.SetDefault("server.address", DefaultAddress)
	v.SetDefault("server.port", config.DefaultPort)
	v.SetDefault("server.metrics", DefaultMetrics)
	v.SetDefault("server.frame_size_bytes", DefaultFrameSizeBytes)
	v.SetDefault("server.keepalive.time", DefaultKeepaliveTime)
	v.SetDefault("server.keepalive.timeout", DefaultKeepaliveTimeout)
	v.SetDefault("server.keepalive.min_time", DefaultKeepaliveMinTime)
	v.SetDefault("server.keepalive.permit_without_stream", DefaultKeepalivePermitWithoutStream)
	v.SetDefault("server.subscribe_buffer_size", DefaultServerSubscribeBufferSize)
	v.SetDefault("server.subscribe_heartbeat_interval", DefaultServerSubscribeHeartbeatInterval)
	v.SetDefault("server.session.grace_period", DefaultSessionGracePeriod)
	v.SetDefault("server.session.idempotency_cache_size", DefaultSessionIdempotencyCacheSize)
	v.SetDefault("server.identity.executor_workers", DefaultIdentityExecutorWorkers)
	v.SetDefault("server.tls.min_version", DefaultTLSMinVersion)
	v.SetDefault("server.ops.addr", DefaultOpsAddr)
	v.SetDefault("server.ops.auth.type", DefaultOpsAuthType)
	v.SetDefault("server.grpc.reflection", DefaultReflection)
	v.SetDefault("server.grpc.limits.max_recv_message_size", DefaultMaxMessageBytes)
	v.SetDefault("server.grpc.limits.max_concurrent_streams", DefaultMaxConcurrentStreams)
	v.SetDefault("server.grpc.limits.max_connection_idle", DefaultMaxConnectionIdle)
	v.SetDefault("server.grpc.limits.max_connection_age", DefaultMaxConnectionAge)

	cfg.Address = v.GetString("server.address")
	cfg.Port = v.GetUint("server.port")
	cfg.Metrics = v.GetBool("server.metrics")
	cfg.PlainMetricsAddr = v.GetString("server.plain_metrics_addr")
	cfg.Pprof = v.GetBool("server.pprof")
	cfg.FrameSizeBytes = v.GetInt("server.frame_size_bytes")
	cfg.Keepalive = ServerKeepaliveConfig{
		Time:                v.GetDuration("server.keepalive.time"),
		Timeout:             v.GetDuration("server.keepalive.timeout"),
		MinTime:             v.GetDuration("server.keepalive.min_time"),
		PermitWithoutStream: v.GetBool("server.keepalive.permit_without_stream"),
	}
	cfg.Session = SessionConfig{
		GracePeriod:          v.GetDuration("server.session.grace_period"),
		IdempotencyCacheSize: v.GetInt("server.session.idempotency_cache_size"),
	}
	cfg.Identity = IdentityConfig{
		ExecutorWorkers: v.GetInt("server.identity.executor_workers"),
	}
	cfg.SubscribeBufferSize = v.GetInt("server.subscribe_buffer_size")
	cfg.SubscribeHeartbeatInterval = v.GetDuration("server.subscribe_heartbeat_interval")
	cfg.TLS = TLSConfig{
		CertFile:     v.GetString("server.tls.cert_file"),
		KeyFile:      v.GetString("server.tls.key_file"),
		ClientCAFile: v.GetString("server.tls.client_ca_file"),
		MinVersion:   v.GetString("server.tls.min_version"),
		Disabled:     v.GetBool("server.tls.disabled"),
	}
	cfg.Ops = parseOpsConfig(v)
	cfg.GRPC = GRPCConfig{
		Reflection: v.GetBool("server.grpc.reflection"),
		Limits: LimitsConfig{
			MaxRecvMessageSize:   v.GetInt("server.grpc.limits.max_recv_message_size"),
			MaxConcurrentStreams: uint32(v.GetUint("server.grpc.limits.max_concurrent_streams")),
			MaxConnectionIdle:    v.GetDuration("server.grpc.limits.max_connection_idle"),
			MaxConnectionAge:     v.GetDuration("server.grpc.limits.max_connection_age"),
		},
	}
	return cfg, nil
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
