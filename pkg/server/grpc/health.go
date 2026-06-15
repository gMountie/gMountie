package grpc

import (
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// HealthService wraps grpc/health's Server with a SetServing shim the
// graceful-shutdown path can call to advertise NOT_SERVING before the
// gRPC server stops accepting requests.
type HealthService struct {
	srv *health.Server
	// serving mirrors the overall gRPC health status so readiness checks can
	// consult it cheaply without an RPC. SetNotServing flips it false during
	// graceful shutdown (OB-M1).
	serving atomic.Bool
}

// NewHealthService constructs a HealthService that reports SERVING for
// the overall server status by default.
func NewHealthService() *HealthService {
	s := health.NewServer()
	// Empty service name = overall server status, per the gRPC health spec.
	s.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	h := &HealthService{srv: s}
	h.serving.Store(true)
	return h
}

// Serving reports whether the gRPC server is advertising SERVING. The ops
// readiness checker consults it so /readyz goes 503 once shutdown begins.
func (h *HealthService) Serving() bool { return h.serving.Load() }

// Register registers the wrapped health server with gRPC. Implements
// the ServiceRegistrar shape so AppContext can plug it in alongside
// other controllers.
func (h *HealthService) Register(s *grpc.Server) {
	healthpb.RegisterHealthServer(s, h.srv)
}

// SetNotServing flips the overall status. Used during graceful shutdown
// so probes drain before GracefulStop runs.
func (h *HealthService) SetNotServing() {
	h.serving.Store(false)
	h.srv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
}
