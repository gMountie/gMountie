//go:build darwin || cgofuse

package mount

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	cgofuse "github.com/winfsp/cgofuse/fuse"
	"go.uber.org/zap"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/io/cgofs"
	"go.gmountie.dev/gmountie/pkg/utils/log"
)

// cgofuseHandle wraps a cgofuse FileSystemHost goroutine as a mountHandle.
type cgofuseHandle struct {
	host *cgofuse.FileSystemHost
	fs   *cgofs.MountieCgoFS
}

func (h *cgofuseHandle) Wait() { <-h.fs.Done() }

func (h *cgofuseHandle) Unmount(mountPath string) error {
	if !h.host.Unmount() {
		return errors.Errorf("cgofuse unmount %s failed", mountPath)
	}
	<-h.fs.Done()
	return nil
}

// establishMount mounts via cgofuse (macOS now; Windows later). Builds the
// adapter, starts the FUSE host in a goroutine, and blocks until the adapter's
// Init fires (mount live) or a timeout elapses. Same signature as the go-fuse
// establishMount so single.go is platform-agnostic.
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration) (mountHandle, error) {
	provider, err := detectProvider(fuseProvider(cfg.Provider), pathExists)
	if err != nil {
		return nil, err
	}
	adapter := cgofs.New(backend, rewriter, metaTimeout)
	host := cgofuse.NewFileSystemHost(adapter)
	opts := macOSMountOptions(volume, provider)

	go func() {
		// Mount blocks until the volume is unmounted; ok==false means the
		// mount never came up. Destroy (-> Done) fires on exit either way.
		if ok := host.Mount(mountPath, opts); !ok {
			log.Log.Error("cgofuse mount exited without success",
				zap.String("volume", volume), zap.String("mount_path", mountPath))
		}
		adapter.Destroy() // ensure Done closes even if cgofuse skipped Destroy
	}()

	select {
	case <-adapter.Ready():
		log.Log.Info("cgofuse mount live",
			zap.String("volume", volume), zap.String("provider", string(provider)))
		return &cgofuseHandle{host: host, fs: adapter}, nil
	case <-adapter.Done():
		// Mount returned false without ever calling Init — fail promptly.
		return nil, fmt.Errorf("cgofuse mount of %s failed to start (provider=%s)", volume, provider)
	case <-time.After(15 * time.Second):
		host.Unmount()
		return nil, fmt.Errorf("cgofuse mount of %s did not become ready within 15s (provider=%s)", volume, provider)
	}
}
