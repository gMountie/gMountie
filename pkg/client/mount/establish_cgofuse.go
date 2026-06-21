//go:build darwin || cgofuse

package mount

import (
	"fmt"
	"runtime"
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

// establishCgoFuse mounts via cgofuse (FUSE-T on macOS; Linux libfuse benchmark).
// Builds the adapter, starts the FUSE host in a goroutine, and blocks until the
// adapter's Init fires (mount live) or a timeout elapses. Same signature as
// establishGoFuse so single.go is platform-agnostic via establishMount dispatchers.
func establishCgoFuse(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration) (mountHandle, error) {
	opts, providerLabel, err := cgofuseMountSetup(volume, cfg, maxWrite)
	if err != nil {
		return nil, err
	}
	adapter := cgofs.New(backend, rewriter)
	host := cgofuse.NewFileSystemHost(adapter)

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
			zap.String("volume", volume), zap.String("provider", providerLabel))
		return &cgofuseHandle{host: host, fs: adapter}, nil
	case <-adapter.Done():
		// Mount returned false without ever calling Init — fail promptly.
		return nil, fmt.Errorf("cgofuse mount of %s failed to start (provider=%s)", volume, providerLabel)
	case <-time.After(15 * time.Second):
		host.Unmount()
		return nil, fmt.Errorf("cgofuse mount of %s did not become ready within 15s (provider=%s)", volume, providerLabel)
	}
}

// cgofuseMountSetup resolves the platform-specific FUSE provider and mount
// options. On macOS it detects macFUSE/FUSE-T and builds macOS mount options
// (volname/local/noappledouble). On other platforms — Linux libfuse (the
// cgofuse-on-linux benchmark and the future "unify Linux" path) and Windows
// WinFsp later — those macOS options are invalid (libfuse rejects volname), so
// the portable Linux options are used instead. Keyed on runtime.GOOS because
// this file builds for both darwin and any platform with -tags cgofuse.
func cgofuseMountSetup(volume string, cfg *config.FUSEConfig, maxWrite int) (opts []string, providerLabel string, err error) {
	if runtime.GOOS == "darwin" {
		provider, derr := detectProvider(fuseProvider(cfg.Provider), pathExists)
		if derr != nil {
			return nil, "", derr
		}
		return macOSMountOptions(volume, provider, maxWrite), string(provider), nil
	}
	return linuxCgofuseOptions(maxWrite), "libfuse", nil
}
