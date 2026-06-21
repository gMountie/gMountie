//go:build darwin

package mount

import (
	"time"

	"github.com/pkg/errors"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"
)

// establishMount on darwin selects the adapter by detected backend: macFUSE uses
// go-fuse (cgo-free code, full node.go features), FUSE-T uses cgofuse (kextless).
// detectProvider honors the fuse.provider config (auto → probe dylibs).
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration, defaultPermissions bool) (mountHandle, error) {
	provider, err := detectProvider(fuseProvider(cfg.Provider), pathExists)
	if err != nil {
		return nil, errors.Wrap(err, "detect FUSE provider")
	}
	switch adapterForProvider(provider) {
	case adapterCgoFuse:
		return establishCgoFuse(mountPath, volume, endpoint, backend, rewriter, cfg, maxWrite, metaTimeout, defaultPermissions)
	default:
		return establishGoFuse(mountPath, volume, endpoint, backend, rewriter, cfg, maxWrite, metaTimeout, defaultPermissions)
	}
}
