package grpc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	clienttls "go.gmountie.dev/gmountie/pkg/client/tls"
	"go.gmountie.dev/gmountie/pkg/common/grpc/snappy"
	serverconfig "go.gmountie.dev/gmountie/pkg/server/config"

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

// newUnconnectedClient builds a Client dialed at endpoint but does NOT run the
// session handshake. Split out so the referral path (NewClientForVolume) can
// call Resolve on the connection before deciding where to complete the session.
func newUnconnectedClient(cfg *config.Config, endpoint string) (Client, error) {
	if cfg == nil || cfg.Server == nil || cfg.Auth == nil {
		return nil, errors.New("config is empty or auth config is empty")
	}
	// NOTE: the client factory deliberately does NOT reconfigure the logger
	// from cfg.Log — that's a CLI concern (cmd/commands applies it after
	// config parsing). A library consumer constructing clients keeps its own
	// logger untouched.
	authConfig := cfg.Auth

	opts := make([]ClientOption, 0)

	if cfg.Rpc != nil {
		opts = append(opts, WithTimeouts(cfg.Rpc.TimeoutMeta, cfg.Rpc.TimeoutIO))
		opts = append(opts, WithRetryWindow(cfg.Rpc.RetryWindow))
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
	tlsCfg, err := clienttls.BuildConfig(clienttls.Config{
		Endpoint:            endpoint,
		Mode:                cfg.Server.TLS.Verify,
		CAFile:              cfg.Server.TLS.CAFile,
		ExpectedFingerprint: cfg.Server.TLS.ExpectedFingerprint,
		ServerName:          cfg.Server.TLS.ServerName,
		KnownHostsPath:      cfg.Server.TLS.KnownHostsPath,
		CertFile:            cfg.Server.TLS.CertFile,
		KeyFile:             cfg.Server.TLS.KeyFile,
		CAPEM:               cfg.Server.TLS.CAPEM,
		CertPEM:             cfg.Server.TLS.CertPEM,
		KeyPEM:              cfg.Server.TLS.KeyPEM,
	})
	if err != nil {
		return nil, errors.Wrap(err, "build client TLS config")
	}
	opts = append(opts, WithDialOptions([]grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	}))

	// Build and register client metrics for this factory call. Register
	// tolerates AlreadyRegisteredError so tests building multiple clients
	// against the default registerer do not panic — it adopts the existing
	// series. RegisterInstance adds m to the per-instance dispatcher so the
	// io/cache layers (which call metrics.OnRetry, CacheHit, etc.) fan-out
	// to every live client without overwriting a shared global hook.
	m := metrics.NewMetrics()
	if err := m.Register(prometheus.DefaultRegisterer); err != nil {
		return nil, errors.Wrap(err, "register client metrics")
	}
	metrics.RegisterInstance(m)
	opts = append(opts, WithMetrics(m))

	// Attach per-RPC basic-auth only for type:basic. For type:mtls the
	// transport client certificate (built into tlsCfg above from
	// cfg.Server.TLS.Cert/Key) is the identity — the server's mtls auth reads
	// the cert CN — so no basic-auth credential metadata is sent.
	if c, ok := authConfig.(*config.BasicAuthConfig); ok && authConfig.GetType() == serverconfig.AuthConfigTypeBasic {
		opts = append(opts, WithBasicAuth(c.Username, c.Password))
	}

	return NewClient(endpoint, opts...)
}

// NewClientFromConfig builds a client to the configured endpoint and completes
// the session handshake. Returns an error if the handshake fails — without it,
// every fd-carrying RPC would be rejected by the server. For a session-bearing
// client to a known endpoint that does not follow referrals; the pre-session
// paths (ls → ListVolumes, mount's referral resolver) skip the handshake.
func NewClientFromConfig(cfg *config.Config) (Client, error) {
	if cfg == nil || cfg.Server == nil {
		return nil, errors.New("config is empty or auth config is empty")
	}
	client, err := newUnconnectedClient(cfg, createEndpoint(cfg.Server))
	if err != nil {
		return nil, err
	}
	if err := client.Connect(); err != nil {
		_ = client.Close()
		return nil, errors.Wrap(err, "session handshake failed; client unusable")
	}
	return client, nil
}

// ListVolumes lists the volumes the caller may mount, over a PRE-SESSION
// VolumeService.List call (no session handshake). This works against both a full
// data server and the cloud resolver (mount.<domain>), which implements only
// VolumeService — there is no SessionService to handshake against there. The
// caller is authenticated per-RPC by the mTLS client cert or basic-auth creds
// the unconnected client wires onto the dial, so no session_id is needed to list.
func ListVolumes(cfg *config.Config) ([]string, error) {
	if cfg == nil || cfg.Server == nil {
		return nil, errors.New("config is empty")
	}
	client, err := newUnconnectedClientFn(cfg, createEndpoint(cfg.Server))
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), resolveMetaTimeout(cfg))
	defer cancel()

	return listVolumes(ctx, client)
}

// resolveTimeout bounds the pre-session Resolve RPC when the config carries no
// explicit meta timeout. A referral lookup is one cheap round-trip.
const resolveTimeout = 5 * time.Second

// newUnconnectedClientFn is the un-connected client builder, indirected through
// a package var so referral orchestration tests can stub the dial.
var newUnconnectedClientFn = newUnconnectedClient

// NewClientForVolume connects to wherever a volume lives. It builds an
// un-connected client to the configured endpoint, calls Resolve (pre-session),
// then completes the session handshake on the configured endpoint when the
// location is empty ("served here") or on a freshly-dialed connection to the
// referred location otherwise. A Resolve failure is fatal — there is no silent
// fallback (the configured endpoint may be a resolve-only control plane).
//
// When volume is empty the caller's volumes are listed first (pre-session, over
// the same resolver connection): exactly one volume is auto-selected, zero or
// many is an error. It returns the connected client and the resolved volume
// name — callers that passed an empty volume need the discovered name for the
// mount call, state file, and logs.
func NewClientForVolume(cfg *config.Config, volume string) (Client, string, error) {
	if cfg == nil || cfg.Server == nil {
		return nil, "", errors.New("config is empty")
	}
	resolver, err := newUnconnectedClientFn(cfg, createEndpoint(cfg.Server))
	if err != nil {
		return nil, "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), resolveMetaTimeout(cfg))
	defer cancel()

	// Auto-resolve the volume when the user omitted --volume: list the caller's
	// volumes over the resolver connection we already hold and mount the sole
	// one. Reuses the same connection and meta-timeout budget as the Resolve
	// below — no second dial.
	if volume == "" {
		names, err := listVolumes(ctx, resolver)
		if err != nil {
			_ = resolver.Close()
			return nil, "", errors.Wrap(err, "list volumes for this credential")
		}
		switch len(names) {
		case 1:
			volume = names[0]
		case 0:
			_ = resolver.Close()
			return nil, "", errors.New("no volumes available for this credential")
		default:
			_ = resolver.Close()
			return nil, "", errors.Errorf("multiple volumes available, pass --volume: %s", strings.Join(names, ", "))
		}
	}

	location, err := resolveLocation(ctx, resolver, volume)
	if err != nil {
		_ = resolver.Close()
		return nil, "", errors.Wrap(err, "resolve volume location")
	}

	if location == "" {
		if err := resolver.Connect(); err != nil {
			_ = resolver.Close()
			return nil, "", errors.Wrap(err, "session handshake failed; client unusable")
		}
		return resolver, volume, nil
	}

	// Referral: drop the resolver connection and dial the data plane.
	_ = resolver.Close()
	dataCfg, err := tlsConfigForReferral(cfg, location)
	if err != nil {
		return nil, "", err
	}
	data, err := newUnconnectedClientFn(dataCfg, location)
	if err != nil {
		return nil, "", err
	}
	if err := data.Connect(); err != nil {
		_ = data.Close()
		return nil, "", errors.Wrap(err, "session handshake failed; client unusable")
	}
	return data, volume, nil
}

// resolveMetaTimeout picks the per-RPC timeout for the Resolve lookup: the
// configured meta timeout when present, else resolveTimeout.
func resolveMetaTimeout(cfg *config.Config) time.Duration {
	if cfg.Rpc != nil && cfg.Rpc.TimeoutMeta > 0 {
		return cfg.Rpc.TimeoutMeta
	}
	return resolveTimeout
}

// createEndpoint creates the endpoint from the client config
func createEndpoint(cfg *config.ServerConfig) string {
	return fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)
}
