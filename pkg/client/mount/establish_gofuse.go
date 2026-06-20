//go:build !darwin && !cgofuse

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

// establishMount mounts via go-fuse (Linux). Mirrors the prior inline body of
// SingleVolumeMounterImpl.Mount.
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration) (mountHandle, error) {
	root := io.NewMountieRoot(backend, rewriter, cfg.DirectIO)
	mountOpts := createMountOptions(endpoint, volume, cfg, maxWrite)
	fsOpts := buildFSOptions(mountOpts, cfg)
	server, err := gofs.Mount(mountPath, root, fsOpts)
	if err := wrapMountError(err); err != nil {
		return nil, errors.Wrap(err, "mount fail")
	}
	return &gofuseHandle{server: server}, nil
}
