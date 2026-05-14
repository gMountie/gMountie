package grpc

import (
	"fmt"
	"gmountie/pkg/client/config"
	serverConfig "gmountie/pkg/server/config"

	"github.com/pkg/errors"
)

// NewClientFromConfig creates a new gRPC ClientImpl from the config
func NewClientFromConfig(cfg *config.Config) (Client, error) {
	if cfg == nil || cfg.Server == nil || cfg.Auth == nil {
		return nil, errors.New("config is empty or auth config is empty")
	}
	authConfig := cfg.Auth

	opts := make([]ClientOption, 0)

	if cfg.Rpc != nil {
		opts = append(opts, WithTimeouts(cfg.Rpc.TimeoutMeta, cfg.Rpc.TimeoutIO))
	}

	switch c := authConfig.(type) {
	case *serverConfig.NoneAuthConfig:
		// Do nothing
	case *config.BasicAuthConfig:
		opts = append(opts, WithBasicAuth(c.Username, c.Password))
	}
	return NewClient(createEndpoint(cfg.Server), opts...)
}

// createEndpoint creates the endpoint from the client config
func createEndpoint(cfg *config.ServerConfig) string {
	return fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)
}
