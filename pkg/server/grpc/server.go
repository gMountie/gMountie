package grpc

import (
	"context"
	"fmt"
	"net"

	grpc2 "go.gmountie.dev/gmountie/pkg/common/grpc"
	"go.gmountie.dev/gmountie/pkg/server/config"
	_ "go.gmountie.dev/gmountie/pkg/server/grpc/snappy" // Installing the snappy encoding as an available compressor.
	"go.gmountie.dev/gmountie/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/pkg/errors"
	prometheus2 "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // Installing the gzip encoding as an available compressor.
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// ServiceRegistrar is an interface that defines the ServiceRegistrar method.
type ServiceRegistrar interface {
	Register(*grpc.Server)
}

// Server is a struct that contains a gRPC server.
type Server struct {
	config      *config.Config
	services    []ServiceRegistrar
	server      *grpc.Server
	authService service.AuthService
	// sessionManager is the SAME instance used by the SessionController so
	// that principals written at Create are visible to the AuthInterceptor.
	sessionManager          service.SessionManager
	listener                net.Listener
	creds                   credentials.TransportCredentials
	extraUnaryInterceptors  []grpc.UnaryServerInterceptor
	extraStreamInterceptors []grpc.StreamServerInterceptor
	metricsServer           *prometheus.ServerMetrics
	// HealthService implements grpc.health.v1.Health. It's always set so
	// probes work regardless of the metrics toggle, and the
	// graceful-shutdown path flips it to NOT_SERVING before GracefulStop.
	HealthService *HealthService
}

// ServerOption is a type that defines the ServerOption function.
type ServerOption func(*Server)

// WithListener sets the listener for the gRPC server.
func WithListener(lis net.Listener) ServerOption {
	return func(s *Server) {
		s.listener = lis
	}
}

// WithCredentials sets the transport credentials (TLS) for the gRPC server.
// When nil or not provided, the server uses plaintext (only valid for
// loopback-bound listeners — the bootstrap enforces this).
func WithCredentials(creds credentials.TransportCredentials) ServerOption {
	return func(s *Server) {
		s.creds = creds
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

// WithSessionManager sets the SessionManager that the AuthInterceptor uses to
// look up session principals. It must be the same instance as the one passed to
// the SessionController so that sessions created via Create are visible here.
func WithSessionManager(mgr service.SessionManager) ServerOption {
	return func(s *Server) {
		s.sessionManager = mgr
	}
}

// NewServer creates a new gRPC server.
func NewServer(config *config.Config, authService service.AuthService, services []ServiceRegistrar, options ...ServerOption) *Server {
	s := &Server{
		config:        config,
		services:      services,
		authService:   authService,
		HealthService: NewHealthService(),
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
	// Initialize Prometheus metrics (collector + interceptors).
	s.initMetricsServer()

	// Create a new gRPC server.
	s.server = grpc.NewServer(s.getOptions()...)
	// Register the services.
	for _, svc := range s.services {
		svc.Register(s.server)
	}
	// Register the gRPC health service. Always on — probes don't depend
	// on the metrics toggle.
	s.HealthService.Register(s.server)
	// Add reflection only when explicitly enabled — off by default so
	// production deployments don't expose the service surface to anonymous callers.
	if s.config != nil && s.config.Server != nil && s.config.Server.GRPC.Reflection {
		reflection.Register(s.server)
	}
	// Log enabled services.
	for name := range s.server.GetServiceInfo() {
		log.Log.Info("gRPC service is enabled", zap.String("service", name))
	}
	log.Log.Info("gRPC server is running", zap.String("address", lis.Addr().String()))
	// Finalize Prometheus metrics initialization now that handlers are
	// registered (NoOp when metrics are disabled).
	if s.metricsServer != nil {
		s.metricsServer.InitializeMetrics(s.server)
	}
	// Serve the gRPC server.
	return s.server.Serve(lis)
}

// Stop stops the gRPC server.
func (s *Server) Stop(gracefully bool) {
	if s.server == nil {
		return
	}
	// Flip health to NOT_SERVING so probes drain before we stop
	// accepting requests.
	if s.HealthService != nil {
		s.HealthService.SetNotServing()
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
	// ListenConfig.Listen is the context-aware form of net.Listen. Same
	// behaviour today (we pass Background) — the linter just wants the
	// ctx-aware shape so a future refactor can plumb cancellation.
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp",
		fmt.Sprintf("%s:%v", s.config.Server.Address, s.config.Server.Port))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create listener")
	}
	return lis, nil
}

// getOptions returns the gRPC server options.
func (s *Server) getOptions() []grpc.ServerOption {
	unaryLog, streamLog := s.getLoggingInterceptor()
	authInterceptor := NewAuthInterceptor(s.authService, s.sessionManager)

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

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
	}
	// Wire TLS credentials when provided. When nil the server uses plaintext
	// (only permitted for loopback-bound listeners; the bootstrap enforces this).
	if s.creds != nil {
		opts = append(opts, grpc.Creds(s.creds))
	}
	// Wire keepalive + message-size guards from config. The server pings idle
	// connections every Time and tears them down after Timeout without an ack.
	// EnforcementPolicy keeps overly chatty clients honest. MaxRecv/SendMsgSize
	// caps protect against runaway payloads. MaxConnectionIdle/Age from
	// GRPC.Limits are folded into the same ServerParameters so there is only
	// ever one KeepaliveParams option and the fields don't clobber each other.
	if s.config != nil && s.config.Server != nil {
		srv := s.config.Server
		kaParams := keepalive.ServerParameters{
			Time:    srv.Keepalive.Time,
			Timeout: srv.Keepalive.Timeout,
		}
		if srv.GRPC.Limits.MaxConnectionIdle > 0 {
			kaParams.MaxConnectionIdle = srv.GRPC.Limits.MaxConnectionIdle
		}
		if srv.GRPC.Limits.MaxConnectionAge > 0 {
			kaParams.MaxConnectionAge = srv.GRPC.Limits.MaxConnectionAge
		}
		opts = append(opts,
			grpc.KeepaliveParams(kaParams),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             srv.Keepalive.MinTime,
				PermitWithoutStream: srv.Keepalive.PermitWithoutStream,
			}),
		)
		// Legacy MaxMessageBytes still drives both directions when set; the new
		// GRPC.Limits.MaxRecvMessageSize knob below overrides only the recv
		// side. Skip on zero so test configs and unconfigured servers don't
		// end up with a 0-byte cap that rejects all traffic.
		if srv.MaxMessageBytes > 0 {
			opts = append(opts,
				grpc.MaxRecvMsgSize(srv.MaxMessageBytes),
				grpc.MaxSendMsgSize(srv.MaxMessageBytes),
			)
		}
		// Append per-connection DoS guards: MaxRecvMessageSize and
		// MaxConcurrentStreams. KeepaliveParams for Idle/Age are already
		// folded into kaParams above; limitsServerOptions skips them here.
		limCfg := config.LimitsConfig{
			MaxRecvMessageSize:   srv.GRPC.Limits.MaxRecvMessageSize,
			MaxConcurrentStreams: srv.GRPC.Limits.MaxConcurrentStreams,
		}
		opts = append(opts, limitsServerOptions(limCfg)...)
	}
	return opts
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

// initMetricsServer initializes the gRPC metrics collector and wires
// its interceptors into the chain. Registration against the default
// registerer tolerates re-registration so `go test -count=N` keeps
// working. The HTTP /metrics endpoint is owned by pkg/server/ops, not
// this package.
func (s *Server) initMetricsServer() {
	if s.config.Server == nil || !s.config.Server.Metrics {
		return
	}
	// Add a metrics interceptor.
	s.metricsServer = prometheus.NewServerMetrics()
	// Register the metrics. Tolerate re-registration when the same
	// default registerer is reused across test runs.
	if err := prometheus2.DefaultRegisterer.Register(s.metricsServer); err != nil {
		var already prometheus2.AlreadyRegisteredError
		if !errors.As(err, &already) {
			log.Log.Warn("register grpc server metrics", zap.Error(err))
		}
	}
	// Add the metrics interceptor.
	s.extraUnaryInterceptors = append(s.extraUnaryInterceptors, s.metricsServer.UnaryServerInterceptor())
	s.extraStreamInterceptors = append(s.extraStreamInterceptors, s.metricsServer.StreamServerInterceptor())
}
