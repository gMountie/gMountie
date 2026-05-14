package grpc

import (
	"fmt"
	"gmountie/pkg/client/config"
	"gmountie/pkg/client/metrics"
	serverConfig "gmountie/pkg/server/config"
	"gmountie/pkg/utils/log"
	"os"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// NewClientFromConfig creates a new gRPC Client from the config and
// triggers the session handshake. Returns an error if the handshake fails
// — without it, every fd-carrying RPC would be rejected by the server.
func NewClientFromConfig(cfg *config.Config) (Client, error) {
	if cfg == nil || cfg.Server == nil || cfg.Auth == nil {
		return nil, errors.New("config is empty or auth config is empty")
	}
	if cfg.Log != nil {
		if err := log.Reconfigure(*cfg.Log, os.Stderr); err != nil {
			return nil, errors.Wrap(err, "configure logger")
		}
	}
	authConfig := cfg.Auth

	opts := make([]ClientOption, 0)

	if cfg.Rpc != nil {
		opts = append(opts, WithTimeouts(cfg.Rpc.TimeoutMeta, cfg.Rpc.TimeoutIO))
	}

	// Build and register client metrics once per factory call. Register
	// tolerates AlreadyRegisteredError so tests building multiple clients
	// against the default registerer do not panic. Install the retry hook
	// here too — the io layer can't import metrics state lazily without
	// creating an import cycle, so metrics owns the global hook.
	m := metrics.NewMetrics()
	if err := m.Register(prometheus.DefaultRegisterer); err != nil {
		return nil, errors.Wrap(err, "register client metrics")
	}
	metrics.SetRetryHook(m.RetryInc)
	opts = append(opts, WithUnaryInterceptors(UnaryClientInFlightInterceptor(m)))

	switch c := authConfig.(type) {
	case *serverConfig.NoneAuthConfig:
		// Do nothing
	case *config.BasicAuthConfig:
		opts = append(opts, WithBasicAuth(c.Username, c.Password))
	}

	client, err := NewClient(createEndpoint(cfg.Server), opts...)
	if err != nil {
		return nil, err
	}
	client.Connect()
	if client.SessionID() == "" {
		_ = client.Close()
		return nil, errors.New("session handshake failed; client unusable")
	}
	return client, nil
}

// createEndpoint creates the endpoint from the client config
func createEndpoint(cfg *config.ServerConfig) string {
	return fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)
}
