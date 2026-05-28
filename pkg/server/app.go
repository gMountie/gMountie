package server

import (
	"context"
	"gmountie/pkg/server/config"
	"gmountie/pkg/server/controller"
	"gmountie/pkg/server/grpc"
	"gmountie/pkg/server/io"
	"gmountie/pkg/server/metrics"
	"gmountie/pkg/server/ops"
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
	Bus            io.EventBus
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
	sessionMgr := service.NewSessionManager(service.SessionManagerOptions{Metrics: m})
	bus := io.NewLocalEventBus(io.EventBusOptions{
		BufferSize:        cfg.Server.SubscribeBufferSize,
		HeartbeatInterval: cfg.Server.SubscribeHeartbeatInterval,
		Metrics:           m,
	})
	return &AppContext{
		Config:         cfg,
		VolumeService:  volumeService,
		AuthService:    authService,
		SessionManager: sessionMgr,
		Metrics:        m,
		Bus:            bus,
	}, nil
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

	appCtx, err := NewServerAppContext(cfg)
	if err != nil {
		return errors.Wrap(err, "build app context")
	}
	s := grpc.NewServer(
		cfg,
		appCtx.AuthService,
		appCtx.GetGrpcServices(),
		grpc.WithExtraUnaryInterceptors(
			grpc.UnaryServerMetricsInterceptor(appCtx.Metrics),
		),
	)

	// Build the ops HTTP server (/metrics, /healthz, /readyz, /version).
	readiness := ops.PathReadinessChecker{Path: firstVolumePath(cfg)}
	opsServer := ops.NewServer(cfg.Server.MetricsAddr, readiness, cfg.Server.Pprof)
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
