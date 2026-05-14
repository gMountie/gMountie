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
	server *http.Server
}

// NewServer constructs an ops server bound to addr that delegates
// readiness checks to readiness. The underlying *http.Server is built
// synchronously here so Stop always has a non-nil target regardless of
// goroutine scheduling between Start and Stop.
func NewServer(addr string, readiness ReadinessChecker) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/healthz", LivenessHandler())
	mux.Handle("/readyz", ReadinessHandler(readiness))
	mux.Handle("/version", VersionHandler())
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
