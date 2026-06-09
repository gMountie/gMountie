package config

const (
	// EnvironmentPrefix is the prefix for GMOUNTIE_* environment-variable
	// overrides, applied by both the server and client config loaders.
	EnvironmentPrefix = "GMOUNTIE"

	// DefaultPort is the default gRPC port: the server listens on it and the
	// client dials it when server.port is unset.
	DefaultPort = 9449
)
