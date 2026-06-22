//go:build !darwin && cgofuse

package mount

import (
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/config"
)

// establishMount on the Linux `-tags cgofuse` benchmark build uses cgofuse
// (head-to-head vs go-fuse). Unchanged behavior from before the split.
func establishMount(mountPath, volume, endpoint string, backend backend.FileSystemBackend, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration, defaultPermissions bool) (mountHandle, error) {
	return establishCgoFuse(mountPath, volume, endpoint, backend, cfg, maxWrite, metaTimeout, defaultPermissions)
}
