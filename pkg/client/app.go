package client

import (
	"errors"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/mount"
	"go.gmountie.dev/gmountie/pkg/client/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"
)

// AppContext is a struct that holds the application context.
type AppContext struct {
	client              grpc.Client
	VolumeService       service.VolumeService
	SingleVolumeMounter mount.SingleVolumeMounter
}

// NewAppContext creates a new AppContext. fuseCfg, cacheCfg and walCfg
// must be non-nil — the client config layer always populates them
// (defaults applied when the user's YAML omits the block).
func NewAppContext(client grpc.Client, fuseCfg *config.FUSEConfig, cacheCfg *config.CacheConfig, walCfg *config.WALConfig) *AppContext {
	log.Log.Info("creating app context")
	return &AppContext{
		client: client,
		// rawIDs=false: the programmatic mount path keeps id rewriting on;
		// raw_ids is a CLI opt-out (cmd/commands/mount.go) for now.
		SingleVolumeMounter: mount.NewSingleVolumeMounter(client, fuseCfg, *cacheCfg, *walCfg, false),
		VolumeService:       service.NewVolumeService(client),
	}
}

// Close closes the AppContext
func (a *AppContext) Close() error {
	log.Log.Info("closing app context")
	var errs = make([]error, 0)
	errs = append(errs, a.SingleVolumeMounter.Close())
	errs = append(errs, a.client.Close())
	return errors.Join(errs...)
}
