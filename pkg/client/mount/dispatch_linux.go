//go:build !darwin && !cgofuse

package mount

import (
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"
)

// establishMount on Linux always uses go-fuse (cgo-free).
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration, defaultPermissions bool) (mountHandle, error) {
	return establishGoFuse(mountPath, volume, endpoint, backend, rewriter, cfg, maxWrite, metaTimeout, defaultPermissions)
}
