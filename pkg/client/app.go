package client

import (
	"errors"
	"gmountie/pkg/client/config"
	"gmountie/pkg/client/grpc"
	"gmountie/pkg/client/mount"
	"gmountie/pkg/client/service"
	"gmountie/pkg/utils/log"
)

// AppContext is a struct that holds the application context.
type AppContext struct {
	client              grpc.Client
	VolumeService       service.VolumeService
	SingleVolumeMounter mount.SingleVolumeMounter
	MultiVolumeMounter  mount.VFSVolumeMounter
}

// NewAppContext creates a new AppContext. fuseCfg and cacheCfg must be
// non-nil — the client config layer always populates them (defaults
// applied when the user's YAML omits the block).
func NewAppContext(client grpc.Client, multiMountPath string, fuseCfg *config.FUSEConfig, cacheCfg *config.CacheConfig) *AppContext {
	log.Log.Info("creating app context")
	return &AppContext{
		client:              client,
		// rawIDs=false: the programmatic/UI mount path keeps id rewriting on;
		// raw_ids is a CLI opt-out (cmd/commands/mount.go) for now.
		SingleVolumeMounter: mount.NewSingleVolumeMounter(client, fuseCfg, *cacheCfg, false),
		MultiVolumeMounter:  mount.NewMultiVolumeMounter(client, multiMountPath, fuseCfg, *cacheCfg),
		VolumeService:       service.NewVolumeService(client),
	}
}

// Close closes the AppContext
func (a *AppContext) Close() error {
	log.Log.Info("closing app context")
	var errs = make([]error, 0)
	errs = append(errs, a.SingleVolumeMounter.Close())
	errs = append(errs, a.MultiVolumeMounter.Close())
	errs = append(errs, a.client.Close())
	return errors.Join(errs...)
}
