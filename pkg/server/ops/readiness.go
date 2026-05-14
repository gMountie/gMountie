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

// PathReadinessChecker stats one path. Failing to stat fails readiness.
// Default usage: probe the root of the first configured volume.
type PathReadinessChecker struct{ Path string }

// Ready returns nil when the configured path is statable.
func (p PathReadinessChecker) Ready(_ context.Context) error {
	if p.Path == "" {
		return errors.New("no readiness probe path configured")
	}
	if _, err := os.Stat(p.Path); err != nil {
		return errors.Wrap(err, "readiness stat")
	}
	return nil
}
