package config

const (
	// DefaultAddress is the default address that the server will listen on
	DefaultAddress = "0.0.0.0"
	// DefaultPort is the default port that the server will listen on
	DefaultPort = 9449
	// DefaultMetricsAddr is the default address the ops HTTP server
	// (/metrics, /healthz, /readyz, /version) listens on.
	DefaultMetricsAddr = ":9090"
)

// ServerConfig is a struct that holds the configuration for the server
type ServerConfig struct {
	// Address is the address that the server will listen on
	Address string `validate:"required,ip"`
	// Port is the port that the server will listen on
	Port uint `validate:"required"`
	// Metrics enables the ops HTTP server.
	Metrics bool
	// MetricsAddr is the address the ops HTTP server listens on.
	MetricsAddr string `validate:"hostname_port" mapstructure:"metrics_addr"`
}

