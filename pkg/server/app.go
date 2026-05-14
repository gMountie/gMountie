package server

import (
	"context"
	"gmountie/pkg/server/config"
	"gmountie/pkg/server/controller"
	"gmountie/pkg/server/grpc"
	"gmountie/pkg/server/io"
	"gmountie/pkg/server/io/middleware"
	"gmountie/pkg/server/service"
	"gmountie/pkg/utils/log"
	"runtime"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type AppContext struct {
	// Config is the configuration for the server.
	Config        *config.Config
	VolumeService service.VolumeService
	AuthService   service.AuthService
}

// NewServerAppContext creates a new ServerContext.
func NewServerAppContext(cfg *config.Config) *AppContext {
	volumeService := service.NewVolumeService(cfg, service.WithMiddleware(getVolumeMiddleware()...))
	authService := service.NewAuthServiceFromConfig(cfg.Auth)
	return &AppContext{
		Config:        cfg,
		VolumeService: volumeService,
		AuthService:   authService,
	}
}

// GetGrpcServices returns the gRPC services.
func (c *AppContext) GetGrpcServices() []grpc.ServiceRegistrar {
	return []grpc.ServiceRegistrar{
		controller.NewGrpcServer(c.VolumeService),
		controller.NewRpcFileServer(c.VolumeService),
		controller.NewVolumeService(c.VolumeService),
	}
}

// Start runs the server until ctx is cancelled. On cancellation it triggers a
// graceful shutdown bounded by shutdownDeadline; if that doesn't complete in
// time it forces a stop. Returns the first non-nil error among serve errors
// and shutdown errors.
func Start(ctx context.Context, cfg *config.Config) error {
	const shutdownDeadline = 30 * time.Second

	appCtx := NewServerAppContext(cfg)
	s := grpc.NewServer(
		cfg,
		appCtx.AuthService,
		appCtx.GetGrpcServices(),
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
			log.Log.Info("server shut down gracefully")
			return nil
		case <-time.After(shutdownDeadline):
			log.Log.Warn("graceful shutdown timed out; forcing stop")
			s.Stop(false)
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
