package ops

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"

	"gmountie/pkg/utils/log"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Server is the HTTP "ops" endpoint mounting /metrics, /healthz, /readyz,
// and /version. Owns no business logic — pure routing over supplied
// handlers and the readiness service.
type Server struct {
	server *http.Server
}

// NewServer constructs an ops server bound to addr that delegates
// readiness checks to readiness. enablePprof flips the /debug/pprof/*
// handlers on; off by default because pprof leaks goroutine names and
// can stall the runtime under large captures. The underlying
// *http.Server is built synchronously here so Stop always has a
// non-nil target regardless of goroutine scheduling between Start and
// Stop.
func NewServer(addr string, readiness ReadinessChecker, enablePprof bool) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/healthz", LivenessHandler())
	mux.Handle("/readyz", ReadinessHandler(readiness))
	mux.Handle("/version", VersionHandler())
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
		server: &http.Server{Addr: addr, Handler: mux},
	}
}

// Start blocks running ListenAndServe. Typical callers run it in a
// goroutine. Returns when the server stops.
func (s *Server) Start() {
	log.Log.Info("ops server starting", zap.String("addr", s.server.Addr))
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Log.Error("ops server stopped", zap.Error(err))
	}
}

// Stop initiates graceful shutdown.
func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
