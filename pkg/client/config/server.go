package config

import (
	"go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/spf13/viper"
)

// TLSConfig controls how the client validates the server's certificate.
// See Phase 7 spec §3.2. The three Verify modes:
//   - "verify"   (default) — full chain validation against CAFile or system roots
//   - "tofu"     — trust on first use; pin server's leaf-cert SHA-256 on first
//     successful connect and refuse on mismatch later
//   - "insecure" — skip verification entirely; intended for prototyping only
type TLSConfig struct {
	CAFile              string `mapstructure:"ca_file"`
	Verify              string `mapstructure:"verify" validate:"omitempty,oneof=verify tofu insecure"`
	ExpectedFingerprint string `mapstructure:"expected_fingerprint"`
	ServerName          string `mapstructure:"server_name"`
	KnownHostsPath      string `mapstructure:"known_hosts_path"` // override default XDG path; used by TOFU tests
	CertFile            string `mapstructure:"cert_file"`        // mTLS, reserved for PR 3
	KeyFile             string `mapstructure:"key_file"`         // mTLS, reserved for PR 3

	// Inline PEM alternatives to the *_file paths, for container-native
	// credential injection (e.g. GMOUNTIE_SERVER_TLS_CERT_PEM env var or a
	// mounted Secret). Inline wins; setting both inline and file for one item
	// is an error. File paths still work.
	CAPEM   string `mapstructure:"ca_pem"`
	CertPEM string `mapstructure:"cert_pem"`
	KeyPEM  string `mapstructure:"key_pem"`
}

// ServerConfig is a struct that holds the configuration for the server
type ServerConfig struct {
	// Address is the address that the server will listen on
	Address string `validate:"required,ip"`
	// Port is the port that the server will listen on
	Port uint `validate:"required,gte=1,lte=65535"`
	// Endpoint is "address:port" used as a dial target and TOFU key.
	// Derived from Address and Port if empty.
	Endpoint string `mapstructure:"endpoint"`
	// TLS holds client TLS verification policy.
	TLS TLSConfig `mapstructure:"tls"`
}

// NewServerConfig creates a new ServerConfig with defaults
func NewServerConfig(v *viper.Viper) (*ServerConfig, error) {
	v.SetDefault("port", config.DefaultPort)
	v.SetDefault("tls.verify", "")

	cfg := &ServerConfig{}
	if err := v.UnmarshalExact(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
