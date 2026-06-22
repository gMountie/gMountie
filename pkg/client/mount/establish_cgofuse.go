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
	"go.gmountie.dev/gmountie/pkg/client/fuse/cgofs"
	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/utils/log"
)

// fskitModulePath is the install location of FUSE-T's Apple FSKit backend
// extension. Its presence is the "auto" backend's signal to prefer FSKit over
// the NFS backend (the FSKit mount itself still has to succeed — see the
// fallback in establishCgoFuse — so an installed-but-not-enabled module is
// handled gracefully).
const fskitModulePath = "/Applications/fuse-t.app/Contents/Extensions/FskitSrvModule.appex"

const (
	// cgofuseMountTimeout bounds how long establishCgoFuse waits for a mount to
	// become ready before giving up.
	cgofuseMountTimeout = 15 * time.Second
	// fskitProbeTimeout is the shorter wait used for an FSKit attempt that may
	// fall back to NFS ("auto" mode). A hung FSKit mount (e.g. the extension is
	// installed but not enabled) should fall back to NFS quickly rather than
	// block for the full mount timeout. A healthy FSKit mount becomes ready in
	// ~1-2s, well within this window.
	fskitProbeTimeout = 5 * time.Second
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
// On macOS/FUSE-T it resolves the backend (NFS vs Apple FSKit) from
// cfg.FuseTBackend; in "auto" mode it prefers FSKit when the module is present
// and transparently falls back to the NFS backend if that mount fails (e.g. the
// extension is installed but not enabled). Same signature as establishGoFuse so
// single.go stays platform-agnostic via establishMount dispatchers.
//
// defaultPermissions is accepted for signature symmetry with establishGoFuse but
// not applied here: the cgofuse/FUSE-T path doesn't take the go-fuse
// default_permissions mount option, and the client-side Access cache (always on
// in the cache decorator) covers the Access-amplification reduction for it.
func establishCgoFuse(mountPath, volume, endpoint string, fsBackend io.FileSystemBackend, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration, defaultPermissions bool) (mountHandle, error) {
	_ = defaultPermissions
	opts, label, allowFallback, err := cgofuseMountSetup(volume, cfg, maxWrite, "")
	if err != nil {
		return nil, err
	}
	// An FSKit attempt that can fall back (auto mode) gets a short timeout so a
	// hung mount falls back to NFS quickly; everything else gets the full wait.
	timeout := cgofuseMountTimeout
	if allowFallback {
		timeout = fskitProbeTimeout
	}
	h, mErr := mountCgoFuseOnce(mountPath, volume, fsBackend, opts, label, timeout)
	if mErr != nil && allowFallback {
		log.Log.Warn("FSKit FUSE-T mount failed; falling back to the NFS backend",
			zap.String("volume", volume), zap.Error(mErr))
		nfsOpts, nfsLabel, _, serr := cgofuseMountSetup(volume, cfg, maxWrite, "nfs")
		if serr != nil {
			return nil, serr
		}
		return mountCgoFuseOnce(mountPath, volume, fsBackend, nfsOpts, nfsLabel, cgofuseMountTimeout)
	}
	return h, mErr
}

// mountCgoFuseOnce starts one cgofuse host goroutine and blocks until the
// adapter's Init fires (mount live), the mount exits without starting, or a
// timeout elapses. Returns a non-nil error on the latter two so the caller can
// decide whether to fall back to another backend.
func mountCgoFuseOnce(mountPath, volume string, fsBackend io.FileSystemBackend, opts []string, label string, readyTimeout time.Duration) (mountHandle, error) {
	adapter := cgofs.New(fsBackend)
	host := cgofuse.NewFileSystemHost(adapter)

	go func() {
		// Mount blocks until the volume is unmounted; ok==false means the
		// mount never came up. Destroy (-> Done) fires on exit either way.
		if ok := host.Mount(mountPath, opts); !ok {
			log.Log.Error("cgofuse mount exited without success",
				zap.String("volume", volume), zap.String("mount_path", mountPath),
				zap.String("backend", label))
		}
		adapter.Destroy() // ensure Done closes even if cgofuse skipped Destroy
	}()

	select {
	case <-adapter.Ready():
		log.Log.Info("cgofuse mount live",
			zap.String("volume", volume), zap.String("backend", label))
		return &cgofuseHandle{host: host, fs: adapter}, nil
	case <-adapter.Done():
		// Mount returned false without ever calling Init — fail promptly.
		return nil, fmt.Errorf("cgofuse mount of %s failed to start (%s)", volume, label)
	case <-time.After(readyTimeout):
		host.Unmount()
		return nil, fmt.Errorf("cgofuse mount of %s did not become ready within %s (%s)", volume, readyTimeout, label)
	}
}

// cgofuseMountSetup resolves the platform-specific FUSE provider, FUSE-T backend
// and mount options. ftOverride forces a specific FUSE-T backend ("nfs"/"fskit")
// — the NFS fallback passes "nfs"; "" resolves it from cfg.FuseTBackend. On
// non-darwin (the cgofuse-on-linux benchmark) the macOS options are invalid, so
// the portable Linux options are used. Keyed on runtime.GOOS because this file
// builds for both darwin and any platform with -tags cgofuse.
func cgofuseMountSetup(volume string, cfg *config.FUSEConfig, maxWrite int, ftOverride string) (opts []string, label string, allowFallback bool, err error) {
	if runtime.GOOS != "darwin" {
		return linuxCgofuseOptions(maxWrite), "libfuse", false, nil
	}
	provider, derr := detectProvider(fuseProvider(cfg.Provider), pathExists)
	if derr != nil {
		return nil, "", false, derr
	}
	if provider != providerFuseT {
		// macFUSE uses the go-fuse adapter and never reaches here in practice;
		// build options without a FUSE-T backend just in case.
		return macOSMountOptions(volume, provider, maxWrite, ""), string(provider), false, nil
	}
	ft := ftOverride
	if ft == "" {
		ft, allowFallback, err = resolveFuseTBackend(cfg.FuseTBackend, pathExists)
		if err != nil {
			return nil, "", false, err
		}
	}
	return macOSMountOptions(volume, provider, maxWrite, ft), "fuse-t (" + ft + ")", allowFallback, nil
}

// resolveFuseTBackend maps the configured FUSE-T backend selection to a concrete
// backend and reports whether a failed mount may fall back to NFS. "auto" (the
// default) prefers FSKit when its module is installed (fallback allowed);
// "fskit" forces it and errors up-front if the module is absent (no fallback);
// "nfs" forces the NFS backend.
func resolveFuseTBackend(want string, exists func(string) bool) (backend string, allowFallback bool, err error) {
	switch want {
	case "fskit":
		if !exists(fskitModulePath) {
			return "", false, fmt.Errorf(
				"fuse.fuset_backend=fskit but the FUSE-T FskitSrvModule was not found at %s; "+
					"install FUSE-T with FSKit support (macOS 15+) and enable it in "+
					"System Settings > General > Login Items & Extensions", fskitModulePath)
		}
		return "fskit", false, nil
	case "nfs":
		return "nfs", false, nil
	default: // "auto" or unset
		if exists(fskitModulePath) {
			return "fskit", true, nil
		}
		return "nfs", false, nil
	}
}
