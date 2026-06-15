package ops

import (
	"context"
	"os"

	"github.com/pkg/errors"
)

// ReadinessChecker decides whether /readyz returns 200 or 503.
type ReadinessChecker interface {
	Ready(ctx context.Context) error
}

// ServingChecker reports whether the gRPC server is advertising SERVING.
// grpc.HealthService satisfies it; readiness consults it so /readyz drops to
// 503 the moment graceful shutdown begins.
type ServingChecker interface {
	Serving() bool
}

// MultiPathReadinessChecker stats EVERY configured volume root and, when a
// Health gate is wired, additionally requires the gRPC server to be serving.
// Readiness fails if: no paths are configured, ANY path fails to stat, or the
// server is draining (OB-M1). The all-paths sweep replaces the old single-path
// probe that was blind to every volume but the first.
type MultiPathReadinessChecker struct {
	// Paths are the volume roots to stat. Empty ⇒ not ready (a server with no
	// volumes shouldn't pass /readyz).
	Paths []string
	// Health, when non-nil, gates readiness on the gRPC serving state.
	Health ServingChecker
}

// Ready returns nil when every configured path is statable and the server (if a
// Health gate is set) is serving.
func (p MultiPathReadinessChecker) Ready(_ context.Context) error {
	if len(p.Paths) == 0 {
		return errors.New("no readiness probe paths configured")
	}
	for _, path := range p.Paths {
		if path == "" {
			return errors.New("empty readiness probe path configured")
		}
		if _, err := os.Stat(path); err != nil {
			return errors.Wrapf(err, "readiness stat %q", path)
		}
	}
	if p.Health != nil && !p.Health.Serving() {
		return errors.New("gRPC server is not serving (draining)")
	}
	return nil
}
