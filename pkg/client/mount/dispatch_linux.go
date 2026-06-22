//go:build !darwin && !cgofuse

package mount

import (
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/config"
)

// establishMount on Linux always uses go-fuse (cgo-free).
func establishMount(mountPath, volume, endpoint string, backend backend.FileSystemBackend, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration, defaultPermissions bool) (mountHandle, error) {
	return establishGoFuse(mountPath, volume, endpoint, backend, cfg, maxWrite, metaTimeout, defaultPermissions)
}
