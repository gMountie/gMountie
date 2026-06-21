//go:build !darwin && cgofuse

package mount

import (
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"
)

// establishMount on the Linux `-tags cgofuse` benchmark build uses cgofuse
// (head-to-head vs go-fuse). Unchanged behavior from before the split.
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration, defaultPermissions bool) (mountHandle, error) {
	return establishCgoFuse(mountPath, volume, endpoint, backend, rewriter, cfg, maxWrite, metaTimeout, defaultPermissions)
}
