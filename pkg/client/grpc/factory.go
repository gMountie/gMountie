package grpc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/client/renew"
	clienttls "go.gmountie.dev/gmountie/pkg/client/tls"
	commonconfig "go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/pkg/common/grpc/snappy"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// newRefresherFn builds the certificate refresher; indirected through a
// package var so token-mode integration tests can inject a refresher whose
// HTTP client trusts a self-signed exchange server.
var newRefresherFn = renew.New

// initialExchangeTimeout budgets the synchronous initial certificate exchange.
// It covers the two exchange requests (profile + sign), each already bounded by
// the refresher's 30s HTTP timeout.
const initialExchangeTimeout = time.Minute

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

// renewParts is the shared token-mode certificate machinery for one mount: a
// single ManagedSource (the in-memory client cert) and the Refresher that mints
// into it. setupRenewer builds the pair ONCE per NewClientForVolume /
// NewClientFromConfig call so a referral's two legs (resolver + data plane)
// reuse the same cert and CA instead of each running its own exchange. The zero
// value (both nil) is "no renew / static-cert mode".
type renewParts struct {
	source    *clienttls.ManagedSource
	refresher *renew.Refresher
}

// enabled reports whether token-mode renewal is in effect for this mount.
func (p renewParts) enabled() bool { return p.refresher != nil }

// setupRenewer builds the shared token-mode certificate machinery for one
// mount and performs the SYNCHRONOUS initial exchange. It contains everything
// that must happen exactly once per mount regardless of referral: the
// static-cert mutual-exclusion check, the ManagedSource + Refresher
// construction (via the newRefresherFn seam), and the initial RenewNow that
// delivers the first cert (and, when none is configured, the data CA).
//
// When cfg.Renew is not enabled it returns the zero renewParts and no error —
// callers thread that straight through and the static/no-renew paths stay
// byte-identical to before.
func setupRenewer(cfg *config.Config) (renewParts, error) {
	if !cfg.Renew.Enabled() {
		return renewParts{}, nil
	}
	if cfg.Server.TLS.CertFile != "" || cfg.Server.TLS.CertPEM != "" {
		return renewParts{}, errors.New("renew.endpoint and a static client cert are mutually exclusive")
	}
	source := &clienttls.ManagedSource{}
	refresher := newRefresherFn(renew.Config{
		Endpoint:  cfg.Renew.Endpoint,
		Token:     cfg.Renew.Token,
		TokenFile: cfg.Renew.TokenFile,
		Before:    cfg.Renew.Before,
	}, source)
	ctx, cancel := context.WithTimeout(context.Background(), initialExchangeTimeout)
	err := refresher.RenewNow(ctx)
	cancel()
	if err != nil {
		return renewParts{}, errors.Wrap(err, "initial certificate exchange")
	}
	return renewParts{source: source, refresher: refresher}, nil
}

// newUnconnectedClient builds a single-leg client: it sets up its own renewer
// (one exchange) and dials endpoint, running the renewal loop on the returned
// client. The non-referral paths (NewClientFromConfig, ListVolumes) use it, as
// do the token-mode unit tests. The referral orchestration uses
// buildUnconnectedClient directly so it can SHARE one renewer across both legs.
func newUnconnectedClient(cfg *config.Config, endpoint string) (Client, error) {
	if cfg == nil || cfg.Server == nil || cfg.Auth == nil {
		return nil, errors.New("config is empty or auth config is empty")
	}
	parts, err := setupRenewer(cfg)
	if err != nil {
		return nil, err
	}
	return buildUnconnectedClient(cfg, endpoint, parts, true)
}

// buildUnconnectedClient builds a Client dialed at endpoint but does NOT run the
// session handshake. It takes the shared renewParts (already exchanged) so the
// referral path can thread one ManagedSource + CA through both legs, and runLoop
// to decide whether THIS client owns the renewal loop: only the final, returned
// client runs it, so closing a discarded resolver leg never stops renewal.
//
// Split out so the referral path (NewClientForVolume) can call Resolve on the
// connection before deciding where to complete the session.
func buildUnconnectedClient(cfg *config.Config, endpoint string, parts renewParts, runLoop bool) (Client, error) {
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
		opts = append(opts, WithConnections(cfg.Rpc.Connections))
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
		// Pin the HTTP/2 flow-control windows when configured (>0). gRPC-go's
		// BDP autotuner under-serves a single connection on high-BDP links, so a
		// generous static window lets reads fill a fat pipe; 0 leaves autotuning
		// on. See RpcConfig.InitialConnWindowBytes for the measurements.
		if cfg.Rpc.InitialConnWindowBytes > 0 {
			dialOpts = append(dialOpts, grpc.WithInitialConnWindowSize(int32(cfg.Rpc.InitialConnWindowBytes)))
		}
		if cfg.Rpc.InitialStreamWindowBytes > 0 {
			dialOpts = append(dialOpts, grpc.WithInitialWindowSize(int32(cfg.Rpc.InitialStreamWindowBytes)))
		}
		opts = append(opts, WithDialOptions(dialOpts))
	}

	// Token mode: the shared renewer (built once per mount by setupRenewer)
	// supplies the in-memory client cert and, when none is configured, the data
	// CA. The initial exchange already ran in setupRenewer, so the first TLS
	// handshake on this leg has a cert ready.
	certSource := parts.source
	refresher := parts.refresher
	caPEM := cfg.Server.TLS.CAPEM
	if refresher != nil && caPEM == "" && cfg.Server.TLS.CAFile == "" {
		caPEM = refresher.CAPEM()
	}

	// Token mode with no CA anywhere: refuse to proceed when the effective verify
	// mode does full chain verification against a CA (mode "verify"/empty, no
	// fingerprint pin). With RootCAs unset BuildConfig would silently verify the
	// data-plane server against the system WebPKI roots — a private CA we never
	// trusted. Fail loudly instead. tofu/insecure/fingerprint-pinned configs do
	// not use RootCAs, so they are unaffected.
	if refresher != nil && caPEM == "" && cfg.Server.TLS.CAFile == "" {
		mode := cfg.Server.TLS.Verify
		if (mode == "" || mode == clienttls.ModeVerify) && cfg.Server.TLS.ExpectedFingerprint == "" {
			return nil, errors.New("token mode: certificate exchange returned no ca_pem and no CA is configured")
		}
	}

	// Build TLS transport credentials unconditionally. The verify mode
	// defaults to "verify" (full chain check) when empty; "insecure" skips
	// it for local dev/testing.
	tlsInput := clienttls.Config{
		Endpoint:            endpoint,
		Mode:                cfg.Server.TLS.Verify,
		CAFile:              cfg.Server.TLS.CAFile,
		ExpectedFingerprint: cfg.Server.TLS.ExpectedFingerprint,
		ServerName:          cfg.Server.TLS.ServerName,
		KnownHostsPath:      cfg.Server.TLS.KnownHostsPath,
		CertFile:            cfg.Server.TLS.CertFile,
		KeyFile:             cfg.Server.TLS.KeyFile,
		CAPEM:               caPEM,
		CertPEM:             cfg.Server.TLS.CertPEM,
		KeyPEM:              cfg.Server.TLS.KeyPEM,
	}
	if certSource != nil {
		// set conditionally — a typed-nil interface would enable the callback path
		tlsInput.ClientCertSource = certSource
	}
	tlsCfg, err := clienttls.BuildConfig(tlsInput)
	if err != nil {
		return nil, errors.Wrap(err, "build client TLS config")
	}
	opts = append(opts, WithDialOptions([]grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	}))

	// In token mode, run the renewal loop on the client lifecycle so the
	// in-memory cert is refreshed before expiry; Close cancels it. Only the
	// final client (runLoop) owns the loop — a referral's discarded resolver
	// leg gets runLoop=false, so closing it never stops renewal for the
	// surviving client.
	if refresher != nil && runLoop {
		opts = append(opts, WithBackgroundTask(refresher.Run))
	}

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
	if c, ok := authConfig.(*config.BasicAuthConfig); ok && authConfig.GetType() == commonconfig.AuthConfigTypeBasic {
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
	parts, err := setupRenewer(cfg)
	if err != nil {
		return nil, err
	}
	endpoint := createEndpoint(cfg.Server)
	client, err := newUnconnectedClientFn(cfg, endpoint, parts, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), resolveMetaTimeout(cfg))
	defer cancel()

	names, err := listVolumes(ctx, client)
	if err != nil {
		return nil, annotateMetaErr(endpoint, resolveMetaTimeout(cfg), parts.enabled(), err)
	}
	return names, nil
}

// resolveConnectFloor is the minimum budget for the one-time pre-session
// connect + Resolve (and the pre-session VolumeList). Unlike a steady-state
// metadata op, this leg pays the full cold-start cost — TCP + a fresh mTLS
// handshake to the mount endpoint — which over a slow/high-latency internet link
// routinely needs more than the per-op timeout_meta (5–10s was observed to be
// too little). So the lookup gets a generous floor independent of timeout_meta:
// a stuck routine GetAttr shouldn't wait this long, but a one-time mount-setup
// dial can. An explicitly larger timeout_meta still wins (see resolveMetaTimeout).
const resolveConnectFloor = 30 * time.Second

// annotateMetaErr enriches a failed pre-session resolver RPC (Resolve or
// VolumeList). A DeadlineExceeded here is almost never a slow server — it means
// the gRPC connection to the mount endpoint never came up: the bytes are dropped
// before the server by a firewall, proxy, or VPN (a black hole, not a refusal).
// Naming the endpoint and the likely cause turns an opaque "context deadline
// exceeded" into something the user can act on. Non-timeout errors (auth denied,
// NotFound, …) carry their own meaning and pass through unchanged.
//
// exchanged reports whether a certificate exchange ran first (token mode). When
// it did, it ruled out an auth/credential problem AND proves a different host
// was reachable, so the diagnosis is sharper; with a static cert we only state
// the observable fact and stay honest.
func annotateMetaErr(endpoint string, timeout time.Duration, exchanged bool, err error) error {
	if status.Code(err) != codes.DeadlineExceeded {
		return err
	}
	detail := "the mount endpoint did not respond"
	if exchanged {
		detail = "the credential exchange succeeded, so the mount endpoint is not responding"
	}
	return errors.Wrapf(err,
		"could not reach the mount endpoint %q within %s: %s — check that "+
			"outbound TCP to it is not blocked by a firewall, proxy "+
			"(HTTP(S)_PROXY), or VPN, or raise rpc.timeout_meta for a "+
			"high-latency link",
		endpoint, timeout, detail)
}

// newUnconnectedClientFn is the un-connected client builder, indirected through
// a package var so referral orchestration tests can stub the dial. It carries
// the shared renewParts (so both referral legs reuse one exchanged cert + CA)
// and runLoop (only the final client owns the renewal loop).
var newUnconnectedClientFn = buildUnconnectedClient

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
	// Build the shared token-mode renewer ONCE: one profile+renew exchange for
	// the whole mount, reused by both the resolver leg and (on referral) the
	// data-plane leg. Neither leg runs the renewal loop at construction
	// (runLoop=false); we start it on the surviving final client below so
	// closing the discarded resolver never stops renewal.
	parts, err := setupRenewer(cfg)
	if err != nil {
		return nil, "", err
	}
	endpoint := createEndpoint(cfg.Server)
	resolver, err := newUnconnectedClientFn(cfg, endpoint, parts, false)
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
			return nil, "", errors.Wrap(annotateMetaErr(endpoint, resolveMetaTimeout(cfg), parts.enabled(), err), "list volumes for this credential")
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
		return nil, "", errors.Wrap(annotateMetaErr(endpoint, resolveMetaTimeout(cfg), parts.enabled(), err), "resolve volume location")
	}

	if location == "" {
		if err := resolver.Connect(); err != nil {
			_ = resolver.Close()
			return nil, "", errors.Wrap(err, "session handshake failed; client unusable")
		}
		// Served here: the resolver IS the final client. Start the renewal loop
		// on it (no-op in static/no-renew mode).
		if parts.enabled() {
			launchRenewal(resolver, parts.refresher.Run)
		}
		return resolver, volume, nil
	}

	// Referral: drop the resolver connection and dial the data plane, reusing
	// the SAME renewer (no second exchange — the data leg's TLS presents the
	// cert already minted into parts.source, and trusts the same CA).
	_ = resolver.Close()
	dataCfg, err := tlsConfigForReferral(cfg, location)
	if err != nil {
		return nil, "", err
	}
	data, err := newUnconnectedClientFn(dataCfg, location, parts, false)
	if err != nil {
		return nil, "", err
	}
	if err := data.Connect(); err != nil {
		_ = data.Close()
		return nil, "", errors.Wrap(err, "session handshake failed; client unusable")
	}
	// The data-plane client is the final one: it owns the renewal loop, which
	// survives the resolver Close above (the resolver never ran it).
	if parts.enabled() {
		launchRenewal(data, parts.refresher.Run)
	}
	return data, volume, nil
}

// resolveMetaTimeout picks the timeout for the pre-session connect/Resolve
// lookup: at least resolveConnectFloor, but a larger configured timeout_meta
// wins (a user who widened it for an extreme link gets that). The floor means a
// small per-op timeout_meta never starves the one-time cold mTLS dial.
func resolveMetaTimeout(cfg *config.Config) time.Duration {
	d := resolveConnectFloor
	if cfg.Rpc != nil && cfg.Rpc.TimeoutMeta > d {
		d = cfg.Rpc.TimeoutMeta
	}
	return d
}

// createEndpoint creates the endpoint from the client config
func createEndpoint(cfg *config.ServerConfig) string {
	return fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)
}
