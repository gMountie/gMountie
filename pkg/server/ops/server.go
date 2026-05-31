package ops

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/pprof"
	"os"

	pkgerrors "github.com/pkg/errors"
	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Server is the HTTP "ops" endpoint mounting /metrics, /healthz, /readyz,
// and /version. Owns no business logic — pure routing over supplied
// handlers and the readiness service.
type Server struct {
	server *http.Server
	tls    *tls.Config
}

// NewServer constructs an ops server bound to addr that delegates
// readiness checks to readiness. enablePprof flips the /debug/pprof/*
// handlers on; off by default because pprof leaks goroutine names and
// can stall the runtime under large captures. auth gates all endpoints
// with HTTP Basic auth when non-nil; pass nil for the auth.type: none
// path. The underlying *http.Server is built synchronously here so Stop
// always has a non-nil target regardless of goroutine scheduling between
// Start and Stop.
func NewServer(
	addr string,
	readiness ReadinessChecker,
	enablePprof bool,
	auth *BasicAuth,
	reloadCfg *config.Config,
	vs service.VolumeService,
	sm service.SessionManager,
	rs *service.RevocationStore,
) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/healthz", LivenessHandler())
	mux.Handle("/readyz", ReadinessHandler(readiness))
	mux.Handle("/version", VersionHandler())
	if reloadCfg != nil && vs != nil && sm != nil && rs != nil {
		mux.Handle("/ops/acl/reload", ReloadHandler(reloadCfg, vs, sm, rs))
	}
	if enablePprof {
		// net/http/pprof registers on DefaultServeMux on import; we use a
		// private mux, so attach the handlers explicitly. Order matters
		// for /debug/pprof/ — index handler must come last so the
		// per-profile routes win pattern matching.
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		mux.HandleFunc("/debug/pprof/", pprof.Index)
	}
	return &Server{
		server: &http.Server{Addr: addr, Handler: auth.Wrap(mux)},
	}
}

// ApplyTLS builds a TLS config from the ops TLS settings and attaches it to
// the server. When CertFile and KeyFile are both empty this is a no-op and the
// server remains plain HTTP. When ClientCAFile is set the resulting config
// requires and verifies client certificates (mTLS).
func (s *Server) ApplyTLS(cfg config.OpsTLSConfig) error {
	if cfg.CertFile == "" && cfg.KeyFile == "" {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return pkgerrors.Wrap(err, "load ops TLS keypair")
	}
	tc := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if cfg.ClientCAFile != "" {
		caPEM, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return pkgerrors.Wrap(err, "read ops client CA")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return pkgerrors.New("ops client_ca_file: no valid PEM certificates")
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	s.tls = tc
	s.server.TLSConfig = tc
	return nil
}

// tlsConfig is a test-only accessor for the attached tls.Config (nil = plain HTTP).
func (s *Server) tlsConfig() *tls.Config { return s.tls }

// NewServerWithTLS builds a minimal ops Server with TLS applied. When
// tlsCfg is empty (CertFile/KeyFile both unset) the server is plain HTTP.
// Used by tests; the production path is NewServer(...) followed by ApplyTLS in
// app.go.
func NewServerWithTLS(addr string, tlsCfg config.OpsTLSConfig) (*Server, error) {
	s := &Server{server: &http.Server{Addr: addr, Handler: http.NewServeMux()}}
	if err := s.ApplyTLS(tlsCfg); err != nil {
		return nil, err
	}
	return s, nil
}

// Start blocks running ListenAndServe. Typical callers run it in a
// goroutine. Returns when the server stops.
func (s *Server) Start() {
	log.Log.Info("ops server starting", zap.String("addr", s.server.Addr))
	var err error
	if s.tls != nil {
		// Certs are already embedded in TLSConfig — pass empty strings.
		err = s.server.ListenAndServeTLS("", "")
	} else {
		err = s.server.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Log.Error("ops server stopped", zap.Error(err))
	}
}

// Stop initiates graceful shutdown.
func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
