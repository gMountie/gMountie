//go:build !cgofuse

package mount

import (
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
)

// gofuseHandle wraps a go-fuse server as a mountHandle.
type gofuseHandle struct{ server *fuse.Server }

func (h *gofuseHandle) Wait() { h.server.Wait() }
func (h *gofuseHandle) Unmount(mountPath string) error {
	return stopServer(h.server, mountPath)
}

// establishGoFuse mounts via go-fuse (Linux default; macFUSE on darwin). Mirrors
// the prior inline body of SingleVolumeMounterImpl.Mount.
func establishGoFuse(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration, defaultPermissions bool) (mountHandle, error) {
	// metaTimeout is unused on the go-fuse path (kept for signature symmetry with the cgofuse
	// mounter; retryOp owns the effective per-op deadline).
	root := io.NewMountieRoot(backend, rewriter, cfg.DirectIO)
	mountOpts := createMountOptions(endpoint, volume, cfg, maxWrite, defaultPermissions)
	fsOpts := buildFSOptions(mountOpts, cfg)
	server, err := gofs.Mount(mountPath, root, fsOpts)
	if err := wrapMountError(err); err != nil {
		return nil, errors.Wrap(err, "mount fail")
	}
	return &gofuseHandle{server: server}, nil
}
