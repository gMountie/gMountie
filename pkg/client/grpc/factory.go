package grpc

import (
	"fmt"
	"gmountie/pkg/client/config"
	"gmountie/pkg/client/metrics"
	clienttls "gmountie/pkg/client/tls"
	"gmountie/pkg/server/grpc/snappy"
	"gmountie/pkg/utils/log"
	"os"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// defaultCallOptions builds the DefaultCallOptions for the gRPC dial:
// message-size caps (always) plus an optional UseCompressor when the
// operator opted in. Lives here so the dial site stays readable.
func defaultCallOptions(rpc *config.RpcConfig) []grpc.CallOption {
	opts := []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(rpc.MaxMessageBytes),
		grpc.MaxCallSendMsgSize(rpc.MaxMessageBytes),
	}
	if rpc.Compression == config.CompressionSnappy {
		opts = append(opts, grpc.UseCompressor(snappy.Name))
	}
	return opts
}

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
		opts = append(opts, WithReadahead(cfg.Rpc.ReadaheadChunkBytes, cfg.Rpc.ReadaheadThreshold, cfg.Rpc.ReadaheadWindow))
		opts = append(opts, WithWriteCoalesce(cfg.Rpc.WriteCoalesceBytes))
		// Wire keepalive + message-size caps from RpcConfig. Matching the
		// server's keepalive params lets the client detect dead connections
		// within ~Time+Timeout instead of hanging until TCP gives up.
		dialOpts := []grpc.DialOption{
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                cfg.Rpc.Keepalive.Time,
				Timeout:             cfg.Rpc.Keepalive.Timeout,
				PermitWithoutStream: cfg.Rpc.Keepalive.PermitWithoutStream,
			}),
			grpc.WithDefaultCallOptions(defaultCallOptions(cfg.Rpc)...),
		}
		opts = append(opts, WithDialOptions(dialOpts))
	}

	// Build TLS transport credentials unconditionally. The verify mode
	// defaults to "verify" (full chain check) when empty; "insecure" skips
	// it for local dev/testing.
	tlsServer := cfg.Server
	endpoint := createEndpoint(tlsServer)
	tlsCfg, err := clienttls.BuildConfig(clienttls.Config{
		Endpoint:            endpoint,
		Mode:                tlsServer.TLS.Verify,
		CAFile:              tlsServer.TLS.CAFile,
		ExpectedFingerprint: tlsServer.TLS.ExpectedFingerprint,
		ServerName:          tlsServer.TLS.ServerName,
		KnownHostsPath:      tlsServer.TLS.KnownHostsPath,
	})
	if err != nil {
		return nil, errors.Wrap(err, "build client TLS config")
	}
	opts = append(opts, WithDialOptions([]grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	}))

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
	metrics.SetCacheHitHook(m.CacheHitInc)
	metrics.SetCacheMissHook(m.CacheMissInc)
	metrics.SetCacheDedupeHitHook(m.CacheDedupeHitInc)
	metrics.SetCacheRevalidationHook(m.CacheRevalidationInc)
	metrics.SetSubscribeEventReceivedHook(m.SubscribeEventReceivedInc)
	metrics.SetSubscribeStreamStateHook(m.SubscribeStreamStateSet)
	metrics.SetCacheUnverifiedHook(m.CacheUnverifiedAdd)
	opts = append(opts, WithUnaryInterceptors(UnaryClientInFlightInterceptor(m)))

	if c, ok := authConfig.(*config.BasicAuthConfig); ok {
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
