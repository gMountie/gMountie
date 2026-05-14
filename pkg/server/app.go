package server

import (
	"context"
	"gmountie/pkg/server/config"
	"gmountie/pkg/server/controller"
	"gmountie/pkg/server/grpc"
	"gmountie/pkg/server/io"
	"gmountie/pkg/server/io/middleware"
	"gmountie/pkg/server/metrics"
	"gmountie/pkg/server/service"
	"gmountie/pkg/utils/log"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/pkg/errors"
	prometheus "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type AppContext struct {
	// Config is the configuration for the server.
	Config         *config.Config
	VolumeService  service.VolumeService
	AuthService    service.AuthService
	SessionManager service.SessionManager
	Metrics        *metrics.Metrics
}

// NewServerAppContext creates a new ServerContext.
func NewServerAppContext(cfg *config.Config) *AppContext {
	m := metrics.NewMetrics()
	// Register against the default registerer so the existing /metrics
	// scrape handler picks them up. Register (not MustRegister)
	// tolerates already-registered collectors so the same default
	// registerer survives `go test -count=N`.
	if err := m.Register(prometheus.DefaultRegisterer); err != nil {
		log.Log.Warn("register server metrics", zap.Error(err))
	}

	volumeService := service.NewVolumeService(cfg, service.WithMiddleware(getVolumeMiddleware()...))
	authService := service.NewAuthServiceFromConfig(cfg.Auth)
	sessionMgr := service.NewSessionManager(service.SessionManagerOptions{Metrics: m})
	return &AppContext{
		Config:         cfg,
		VolumeService:  volumeService,
		AuthService:    authService,
		SessionManager: sessionMgr,
		Metrics:        m,
	}
}

// GetGrpcServices returns the gRPC services.
func (c *AppContext) GetGrpcServices() []grpc.ServiceRegistrar {
	return []grpc.ServiceRegistrar{
		controller.NewGrpcServer(c.VolumeService, c.SessionManager),
		controller.NewRpcFileServer(c.VolumeService, c.SessionManager, c.Metrics),
		controller.NewVolumeService(c.VolumeService),
		controller.NewSessionController(c.SessionManager),
	}
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

	appCtx := NewServerAppContext(cfg)
	s := grpc.NewServer(
		cfg,
		appCtx.AuthService,
		appCtx.GetGrpcServices(),
		grpc.WithExtraUnaryInterceptors(
			grpc.UnaryServerMetricsInterceptor(appCtx.Metrics),
		),
	)

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
			return errors.New("shutdown deadline exceeded")
		}
	}
}

// getVolumeMiddleware returns the volume middleware.
func getVolumeMiddleware() []io.Middleware {
	m := make([]io.Middleware, 0)
	// If user is root we can assume the user identity
	if runtime.GOOS == "linux" && syscall.Getuid() == 0 {
		m = append(m, middleware.AssumeUserMiddleware)
	}
	// Print middleware
	names := make([]string, 0, len(m))
	for _, mw := range m {
		names = append(names, mw.GetName())
	}
	if len(names) > 0 {
		log.Log.Info("enabled filesystem middlewares", zap.Strings("middlewares", names))
	}
	return m
}
