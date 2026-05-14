package grpc

import (
	"fmt"
	grpc2 "gmountie/pkg/common/grpc"
	"gmountie/pkg/server/config"
	_ "gmountie/pkg/server/grpc/snappy" // Installing the snappy encoding as an available compressor.
	"gmountie/pkg/server/service"
	"gmountie/pkg/utils/log"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/pkg/errors"
	prometheus2 "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // Installing the gzip encoding as an available compressor.
	"google.golang.org/grpc/reflection"
)

// ServiceRegistrar is an interface that defines the ServiceRegistrar method.
type ServiceRegistrar interface {
	Register(*grpc.Server)
}

// Server is a struct that contains a gRPC server.
type Server struct {
	config                  *config.Config
	services                []ServiceRegistrar
	server                  *grpc.Server
	authService             service.AuthService
	listener                net.Listener
	extraUnaryInterceptors  []grpc.UnaryServerInterceptor
	extraStreamInterceptors []grpc.StreamServerInterceptor
	metricsServer           *prometheus.ServerMetrics
}

// ServerOption is a type that defines the ServerOption function.
type ServerOption func(*Server)

// WithListener sets the listener for the gRPC server.
func WithListener(lis net.Listener) ServerOption {
	return func(s *Server) {
		s.listener = lis
	}
}

// WithExtraUnaryInterceptors appends unary server interceptors to the
// chain built in getOptions(). They run after the built-in
// request-id/log-context/auth/log interceptors.
func WithExtraUnaryInterceptors(unary ...grpc.UnaryServerInterceptor) ServerOption {
	return func(s *Server) {
		s.extraUnaryInterceptors = append(s.extraUnaryInterceptors, unary...)
	}
}

// WithExtraStreamInterceptors appends stream server interceptors to the
// chain built in getOptions().
func WithExtraStreamInterceptors(stream ...grpc.StreamServerInterceptor) ServerOption {
	return func(s *Server) {
		s.extraStreamInterceptors = append(s.extraStreamInterceptors, stream...)
	}
}

// NewServer creates a new gRPC server.
func NewServer(config *config.Config, authService service.AuthService, services []ServiceRegistrar, options ...ServerOption) *Server {
	s := &Server{
		config:      config,
		services:    services,
		authService: authService,
	}

	for _, opt := range options {
		opt(s)
	}
	return s
}

// Serve starts the gRPC server.
func (s *Server) Serve() error {
	// Create a new listener.
	lis, err := s.createListener()
	if err != nil {
		return err
	}
	// Initialize Prometheus metrics.
	s.initMetricsServer()

	// Create a new gRPC server.
	s.server = grpc.NewServer(s.getOptions()...)
	// Register the services.
	for _, svc := range s.services {
		svc.Register(s.server)
	}
	// Add reflection.
	reflection.Register(s.server)
	// Log enabled services.
	for name := range s.server.GetServiceInfo() {
		log.Log.Info("gRPC service is enabled", zap.String("service", name))
	}
	log.Log.Info("gRPC server is running", zap.String("address", lis.Addr().String()))
	// Start the metrics server.
	s.startMetricsServer()
	// Serve the gRPC server.
	return s.server.Serve(lis)
}

// Stop stops the gRPC server.
func (s *Server) Stop(gracefully bool) {
	if s.server == nil {
		return
	}
	if gracefully {
		s.server.GracefulStop()
	} else {
		s.server.Stop()
	}
}

// GetMetricsServer returns the metrics server.
func (s *Server) GetMetricsServer() *prometheus.ServerMetrics {
	return s.metricsServer
}

// createListener creates a new listener.
func (s *Server) createListener() (net.Listener, error) {
	// If the listener is already set, return it.
	if s.listener != nil {
		return s.listener, nil
	}
	// Create a new listener.
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%v", s.config.Server.Address, s.config.Server.Port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create listener")
	}
	return lis, nil
}

// getOptions returns the gRPC server options.
func (s *Server) getOptions() []grpc.ServerOption {
	unaryLog, streamLog := s.getLoggingInterceptor()
	authInterceptor := NewAuthInterceptor(s.authService)

	unaryInterceptors := append(
		[]grpc.UnaryServerInterceptor{
			grpc2.ServerUnaryRequestID(),  // 1. request_id (also injects log field).
			grpc2.ServerUnaryLogContext(), // 2. session_id, volume from request getters.
			authInterceptor.Unary(),       // 3. user (already injects).
			unaryLog,                      // 4. finish-call line w/ all fields.
		},
		s.extraUnaryInterceptors...,
	)

	streamInterceptors := append(
		[]grpc.StreamServerInterceptor{
			authInterceptor.Stream(), // Must be first for the user to be logged.
			streamLog,
		},
		s.extraStreamInterceptors...,
	)

	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
	}
}

// getLoggingInterceptor returns a new logging interceptor.
func (s *Server) getLoggingInterceptor() (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	opts := []logging.Option{
		logging.WithLogOnEvents(logging.FinishCall),
		logging.WithLevels(func(code codes.Code) logging.Level {
			switch code {
			case codes.OK:
				// Because we are getting a lot of OKs, we are going to log them as debug.
				return logging.LevelDebug
			default:
				return logging.DefaultServerCodeToLevel(code)
			}
		}),
		// Add any other option (check functions starting with logging.With).
	}
	unary := logging.UnaryServerInterceptor(grpc2.InterceptorLogger(log.Log), opts...)
	stream := logging.StreamServerInterceptor(grpc2.InterceptorLogger(log.Log), opts...)
	return unary, stream
}

// startMetricsServer starts the metrics server.
func (s *Server) startMetricsServer() {
	if s.metricsServer == nil {
		log.Log.Debug("metrics server is disabled")
		return
	}
	s.metricsServer.InitializeMetrics(s.server)
	// Start the metrics server.
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Log.Info("starting metrics server", zap.String("port", "9090"), zap.String("path", "/metrics"))
		if err := http.ListenAndServe(":9090", nil); err != nil {
			// Best-effort: a metrics-listener failure must not kill the server.
			log.Log.Error("metrics server stopped", zap.Error(err))
		}
	}()
}

// initMetricsServer initializes the metrics server.
func (s *Server) initMetricsServer() {
	if s.config.Server == nil || !s.config.Server.Metrics {
		return
	}
	// Add a metrics interceptor.
	s.metricsServer = prometheus.NewServerMetrics()
	// Register the metrics.
	prometheus2.MustRegister(s.metricsServer)
	// Add the metrics interceptor.
	s.extraUnaryInterceptors = append(s.extraUnaryInterceptors, s.metricsServer.UnaryServerInterceptor())
	s.extraStreamInterceptors = append(s.extraStreamInterceptors, s.metricsServer.StreamServerInterceptor())

}
