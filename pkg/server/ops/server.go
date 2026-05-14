package ops

import (
	"context"
	"errors"
	"net/http"

	"gmountie/pkg/utils/log"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Server is the HTTP "ops" endpoint mounting /metrics, /healthz, /readyz,
// and /version. Owns no business logic — pure routing over supplied
// handlers and the readiness service.
type Server struct {
	addr      string
	readiness ReadinessChecker
	server    *http.Server
}

// NewServer constructs an ops server bound to addr that delegates
// readiness checks to readiness.
func NewServer(addr string, readiness ReadinessChecker) *Server {
	return &Server{addr: addr, readiness: readiness}
}

// Start binds and serves. Returns when ListenAndServe returns. Typical
// callers run this in a goroutine.
func (s *Server) Start() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/healthz", LivenessHandler())
	mux.Handle("/readyz", ReadinessHandler(s.readiness))
	mux.Handle("/version", VersionHandler())

	s.server = &http.Server{Addr: s.addr, Handler: mux}
	log.Log.Info("ops server starting", zap.String("addr", s.addr))
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Log.Error("ops server stopped", zap.Error(err))
	}
}

// Stop initiates graceful shutdown.
func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}
