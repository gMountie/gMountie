package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/adrg/xdg"
	"github.com/pkg/errors"
	prometheus "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"

	"go.gmountie.dev/gmountie/pkg/common/passhash"
	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/controller"
	"go.gmountie.dev/gmountie/pkg/server/grpc"
	"go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/metrics"
	"go.gmountie.dev/gmountie/pkg/server/ops"
	"go.gmountie.dev/gmountie/pkg/server/service"
	servertls "go.gmountie.dev/gmountie/pkg/server/tls"
	"go.gmountie.dev/gmountie/pkg/utils/log"
)

type AppContext struct {
	// Config is the configuration for the server.
	Config         *config.Config
	VolumeService  service.VolumeService
	AuthService    service.AuthService
	SessionManager service.SessionManager
	Metrics        *metrics.Metrics
	Bus            io.EventBus
	// Revocation is the shared cert-serial blocklist. All three readers
	// (TLS handshake hook, gRPC auth interceptor, session reaper) and the
	// ops reload writer share this single pointer so updates are immediately
	// visible without restarting the server.
	Revocation *service.RevocationStore
}

// NewServerAppContext creates a new ServerContext.
func NewServerAppContext(cfg *config.Config) (*AppContext, error) {
	m := metrics.NewMetrics()
	// Register against the default registerer so the existing /metrics
	// scrape handler picks them up. Register (not MustRegister)
	// tolerates already-registered collectors so the same default
	// registerer survives `go test -count=N`.
	if err := m.Register(prometheus.DefaultRegisterer); err != nil {
		log.Log.Warn("register server metrics", zap.Error(err))
	}

	warnIfIdentityEnforcementUnprivileged()
	volumeService, err := service.NewVolumeService(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "build volume service")
	}
	authService := service.NewAuthServiceFromConfig(cfg.Auth)
	sessionMgr := service.NewSessionManager(service.SessionManagerOptions{
		Metrics:     m,
		GracePeriod: cfg.Server.Session.GracePeriod,
	})
	bus := io.NewLocalEventBus(io.EventBusOptions{
		BufferSize:        cfg.Server.SubscribeBufferSize,
		HeartbeatInterval: cfg.Server.SubscribeHeartbeatInterval,
		Metrics:           m,
	})
	revocation := service.NewRevocationStore()
	revocation.Set(revokedSerialsFromConfig(cfg))
	return &AppContext{
		Config:         cfg,
		VolumeService:  volumeService,
		AuthService:    authService,
		SessionManager: sessionMgr,
		Metrics:        m,
		Bus:            bus,
		Revocation:     revocation,
	}, nil
}

// revokedSerialsFromConfig extracts the startup blocklist from config. Both
// basic and mtls auth use *BasicAuthConfig (NewMTLSAuthConfig also returns
// that type), so a single type assertion covers both modes. Loaded at startup
// so a restart is fail-closed — a revoked cert in config stays blocked across
// server restarts without depending on the ops reload handler.
func revokedSerialsFromConfig(cfg *config.Config) []string {
	if bac, ok := cfg.Auth.(*config.BasicAuthConfig); ok {
		return bac.RevokedSerials
	}
	// Defensive: if a future auth type that supports revocation is ever added as
	// a separate concrete type, returning nil here would start the server with
	// an empty blocklist — a silent fail-OPEN on restart. Surface it loudly so
	// it can't pass unnoticed. (Today mtls is *BasicAuthConfig, so unreachable.)
	if cfg.Auth != nil && cfg.Auth.GetType() == config.AuthConfigTypeMTLS {
		log.Log.Error("revokedSerialsFromConfig: mTLS auth config is not *BasicAuthConfig; " +
			"cert-serial blocklist will be EMPTY (fail-open on restart) — wire the new type here")
	}
	return nil
}

// GetGrpcServices returns the gRPC services.
func (c *AppContext) GetGrpcServices() []grpc.ServiceRegistrar {
	return []grpc.ServiceRegistrar{
		controller.NewGrpcServer(c.VolumeService, c.SessionManager, c.Config.Server.CompoundMaxParallel, c.Bus, c.Metrics),
		controller.NewRpcFileServer(c.VolumeService, c.SessionManager, c.Metrics, c.Config.Server.FrameSizeBytes, c.Bus),
		controller.NewVolumeService(c.VolumeService),
		controller.NewSessionController(c.SessionManager, c.VolumeService),
		controller.NewVersionController(c.Config.Server.FrameSizeBytes),
	}
}

// firstVolumePath returns the path of the first configured volume, or
// "" when no volumes are configured. PathReadinessChecker treats the
// empty case as not-ready, which is the desired behaviour: a server
// with no volumes shouldn't pass /readyz.
func firstVolumePath(cfg *config.Config) string {
	if len(cfg.Volumes) == 0 {
		return ""
	}
	return cfg.Volumes[0].Path
}

// newOpsServer builds the ops HTTP server and applies operator mTLS when
// configured. ApplyTLS is part of construction here so a future change cannot
// drop it and silently serve the mutating /ops/acl/reload endpoint as plaintext
// — a missing/invalid keypair fails the build (and thus startup) instead.
func newOpsServer(cfg *config.Config, appCtx *AppContext) (*ops.Server, error) {
	readiness := ops.PathReadinessChecker{Path: firstVolumePath(cfg)}
	srv := ops.NewServer(
		cfg.Server.Ops.Addr,
		readiness,
		cfg.Server.Pprof,
		ops.NewBasicAuth(cfg.Server.Ops.Auth.Users),
		cfg,
		appCtx.VolumeService,
		appCtx.SessionManager,
		appCtx.Revocation,
	)
	if err := srv.ApplyTLS(cfg.Server.Ops.TLS); err != nil {
		return nil, errors.Wrap(err, "configure ops TLS")
	}
	return srv, nil
}

// Start runs the server until ctx is cancelled. On cancellation it triggers a
// graceful shutdown bounded by shutdownDeadline; if that doesn't complete in
// time it forces a stop. Returns the first non-nil error among serve errors
// and shutdown errors.
func Start(ctx context.Context, cfg *config.Config) error {
	const shutdownDeadline = 30 * time.Second

	if cfg.Log != nil {
		if err := log.Reconfigure(*cfg.Log, os.Stderr); err != nil {
			return errors.Wrap(err, "configure logger")
		}
	}

	if err := cfg.ValidateVolumePaths(); err != nil {
		return errors.Wrap(err, "invalid volume configuration")
	}

	appCtx, err := NewServerAppContext(cfg)
	if err != nil {
		return errors.Wrap(err, "build app context")
	}

	// Resolve TLS before building the listener so a cert failure aborts
	// before any socket is bound.
	bind := net.JoinHostPort(cfg.Server.Address, strconv.FormatUint(uint64(cfg.Server.Port), 10))
	var serverCreds credentials.TransportCredentials
	if !cfg.Server.TLS.Disabled {
		certPath, keyPath := resolveCertPaths(cfg.Server.TLS)
		if err := os.MkdirAll(filepath.Dir(certPath), 0o755); err != nil {
			return errors.Wrap(err, "ensure cert dir")
		}
		host := hostFromBind(bind)
		// LoadOrGenerate keeps the generate-on-first-boot + fail-fast
		// behavior; the Reloader then owns the pair so a rotated cert is
		// picked up at the next handshake without a restart.
		_, _, fp, err := servertls.LoadOrGenerate(certPath, keyPath, host)
		if err != nil {
			return errors.Wrap(err, "load/generate server cert")
		}
		reloader, err := servertls.NewReloader(certPath, keyPath)
		if err != nil {
			return errors.Wrap(err, "init server cert reloader")
		}
		tlsCfg := &tls.Config{
			MinVersion:     minTLSVersion(cfg.Server.TLS.MinVersion),
			NextProtos:     []string{"h2"},
			GetCertificate: reloader.GetCertificate,
		}
		// mTLS: require + verify client certificates against the configured CA.
		if cfg.Auth.GetType() == config.AuthConfigTypeMTLS {
			if cfg.Server.TLS.ClientCAFile == "" {
				return errors.New("auth.type: mtls requires server.tls.client_ca_file")
			}
			caPEM, err := os.ReadFile(cfg.Server.TLS.ClientCAFile)
			if err != nil {
				return errors.Wrap(err, "read client CA file")
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return errors.New("server.tls.client_ca_file: no valid PEM certificates found")
			}
			tlsCfg.ClientCAs = pool
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
			tlsCfg.VerifyPeerCertificate = func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
				return rejectIfRevoked(appCtx.Revocation, verifiedChains)
			}
			log.Log.Info("mTLS client authentication enabled",
				zap.String("client_ca_file", cfg.Server.TLS.ClientCAFile))
		}
		serverCreds = credentials.NewTLS(tlsCfg)
		log.Log.Info("server TLS enabled",
			zap.String("cert_path", certPath),
			zap.String("fingerprint", fp))
	} else {
		if cfg.Auth.GetType() == config.AuthConfigTypeMTLS {
			return errors.New("auth.type: mtls is incompatible with server.tls.disabled")
		}
		if !isLoopback(bind) {
			return errors.Errorf("server.tls.disabled=true requires a loopback bind address (got %q)", bind)
		}
		log.Log.Warn("server TLS DISABLED — every connection is plaintext (dev mode)",
			zap.String("bind", bind))
	}

	grpcOpts := []grpc.ServerOption{
		grpc.WithExtraUnaryInterceptors(
			grpc.UnaryServerMetricsInterceptor(appCtx.Metrics),
		),
		// Wire the same SessionManager the SessionController uses so that
		// principals bound at Create are visible to the AuthInterceptor.
		grpc.WithSessionManager(appCtx.SessionManager),
		// Wire the shared RevocationStore so the AuthInterceptor can deny
		// blocked cert serials per-RPC (covers already-established conns).
		grpc.WithRevocation(appCtx.Revocation),
	}
	if serverCreds != nil {
		grpcOpts = append(grpcOpts, grpc.WithCredentials(serverCreds))
	}
	s := grpc.NewServer(
		cfg,
		appCtx.AuthService,
		appCtx.GetGrpcServices(),
		grpcOpts...,
	)

	// Validate ops config before binding any socket.
	if err := validateOpsConfig(cfg.Server.Ops); err != nil {
		return errors.Wrap(err, "ops config")
	}

	// Build the ops HTTP server (/metrics, /healthz, /readyz, /version,
	// /ops/acl/reload) with operator mTLS applied when configured.
	opsServer, err := newOpsServer(cfg, appCtx)
	if err != nil {
		return errors.Wrap(err, "build ops server")
	}
	go opsServer.Start()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.Serve()
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			return errors.Wrap(err, "failed to start server")
		}
		return nil
	case <-ctx.Done():
		log.Log.Info("shutdown signal received; draining in-flight requests",
			zap.Duration("deadline", shutdownDeadline))

		// Flip health to NOT_SERVING immediately so external probes see
		// the drain before GracefulStop runs.
		s.HealthService.SetNotServing()

		stopped := make(chan struct{})
		go func() {
			s.Stop(true)
			close(stopped)
		}()

		select {
		case <-stopped:
			if err := appCtx.SessionManager.Stop(context.Background()); err != nil {
				log.Log.Warn("session manager stop returned error", zap.Error(err))
			}
			appCtx.Bus.Close()
			opsCtx, opsCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := opsServer.Stop(opsCtx); err != nil {
				log.Log.Warn("ops server stop returned error", zap.Error(err))
			}
			opsCancel()
			log.Log.Info("server shut down gracefully")
			return nil
		case <-time.After(shutdownDeadline):
			log.Log.Warn("graceful shutdown timed out; forcing stop")
			s.Stop(false)
			sessCtx, sessCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := appCtx.SessionManager.Stop(sessCtx); err != nil {
				log.Log.Warn("session manager stop returned error", zap.Error(err))
			}
			sessCancel()
			appCtx.Bus.Close()
			opsCtx, opsCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := opsServer.Stop(opsCtx); err != nil {
				log.Log.Warn("ops server stop returned error", zap.Error(err))
			}
			opsCancel()
			return errors.New("shutdown deadline exceeded")
		}
	}
}

// resolveCertPaths returns the cert + key paths to use. When config is unset
// they default to $XDG_STATE_HOME/gmountie/server.{crt,key}. Pure resolution —
// no I/O. Matches the path the `gmountie fingerprint` subcommand reads.
func resolveCertPaths(cfg config.TLSConfig) (certPath, keyPath string) {
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		return cfg.CertFile, cfg.KeyFile
	}
	base := filepath.Join(xdg.StateHome, "gmountie")
	return filepath.Join(base, "server.crt"), filepath.Join(base, "server.key")
}

// minTLSVersion maps a "1.2" / "1.3" string to the tls.Version* constant. The
// caller normalizes empty to "1.3" before calling; unknown values fall through
// to TLS 1.3 with a warning. Validation already restricts to {1.2, 1.3}.
func minTLSVersion(s string) uint16 {
	switch s {
	case "1.2":
		return tls.VersionTLS12
	default:
		return tls.VersionTLS13
	}
}

// hostFromBind extracts the host part of a "host:port" bind string for cert SAN.
// Falls back to "gmountie-server" when no host is given (":9244").
func hostFromBind(bind string) string {
	if bind == "" {
		return "gmountie-server"
	}
	host, _, err := net.SplitHostPort(bind)
	if err != nil || host == "" || host == "0.0.0.0" || host == "::" {
		return "gmountie-server"
	}
	return host
}

// validateOpsConfig checks the ops config for unsafe combinations at startup.
// Rules:
//   - auth.type: none is only allowed on loopback addresses.
//   - auth.type: basic requires at least one user and every user's PasswordHash
//     must be a valid argon2id PHC string.
func validateOpsConfig(ops config.OpsConfig) error {
	authType := ops.Auth.Type
	if authType == "" || authType == "none" {
		if !isLoopback(ops.Addr) {
			return errors.Errorf(
				"server.ops.addr %q requires server.ops.auth.type: basic "+
					"(only loopback addresses can run without auth)",
				ops.Addr,
			)
		}
		return nil
	}
	if authType == "basic" {
		if len(ops.Auth.Users) == 0 {
			return errors.New("server.ops.auth.type: basic requires at least one entry in server.ops.auth.users")
		}
		for i, u := range ops.Auth.Users {
			if !passhash.IsHashed(u.PasswordHash) {
				return errors.Errorf(
					"server.ops.auth.users[%d].password_hash must be a $argon2id$ PHC string; "+
						"run 'gmountie genpass' and paste the output",
					i,
				)
			}
		}
	}
	if authType == "mtls" {
		if ops.TLS.CertFile == "" || ops.TLS.KeyFile == "" || ops.TLS.ClientCAFile == "" {
			return errors.New(
				"server.ops.auth.type: mtls requires server.ops.tls.cert_file, " +
					"key_file and client_ca_file",
			)
		}
		return nil
	}
	return nil
}

// isLoopback returns true when the bind addr is on 127.0.0.0/8 or [::1]. Used
// to gate the tls.disabled escape hatch.
func isLoopback(bind string) bool {
	host, _, err := net.SplitHostPort(bind)
	if err != nil {
		host = bind
	}
	if host == "" || host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// rejectIfRevoked is the VerifyPeerCertificate body: it rejects a verified
// chain whose leaf serial is blocked. Called at the TLS handshake so a newly
// revoked cert is denied on NEW connections immediately; the per-RPC interceptor
// check covers already-established connections. Safe under concurrent handshakes:
// RevocationStore.IsBlocked is an atomic load.
func rejectIfRevoked(rs *service.RevocationStore, verifiedChains [][]*x509.Certificate) error {
	if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
		return nil
	}
	if rs.IsBlocked(service.SerialKey(verifiedChains[0][0].SerialNumber)) {
		return errors.New("client certificate revoked")
	}
	return nil
}

// warnIfIdentityEnforcementUnprivileged emits a loud startup warning when the
// server is running unprivileged on Linux. The per-request identity-bound
// filesystem sets the resolved identity's credentials via
// setfsuid/setfsgid/setgroups, which require root (or CAP_SETUID+CAP_SETGID).
// Without them, BindIdentity skips the identity layer and every operation runs
// as the server's own user — i.e. permission enforcement is DISABLED. We warn
// rather than refuse to start so local development without privileges remains
// possible.
func warnIfIdentityEnforcementUnprivileged() {
	if runtime.GOOS == "linux" && syscall.Geteuid() != 0 {
		log.Log.Warn("server is not running as root; identity enforcement is DISABLED",
			zap.String("detail", "per-request setfsuid/setfsgid/setgroups require root "+
				"(or CAP_SETUID+CAP_SETGID); without them all operations run as the "+
				"server's own user and per-principal permissions are NOT enforced"))
	}
}
