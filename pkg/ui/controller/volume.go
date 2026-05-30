package controller

import (
	"context"
	"fmt"
	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/ui/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/wailsapp/wails/v3/pkg/application"
	"go.uber.org/zap"
)

type VolumeController interface {
	// GetVolumes returns a list of volumes
	GetVolumes() ([]common.Volume, error)
	// IsMounted checks if a volume is mounted
	IsMounted(volume common.Volume) (bool, error)
	// Mount mounts a volume
	Mount(volume common.Volume) error
	// Unmount unmounts a volume
	Unmount(volume common.Volume) error
}

type VolumeControllerImpl struct {
	ctx        context.Context
	appService service.AppService
	vfsMounted bool
}

func NewVolumeControllerImpl(appService service.AppService) *VolumeControllerImpl {
	return &VolumeControllerImpl{
		appService: appService,
		vfsMounted: false,
	}
}

func (v *VolumeControllerImpl) OnStartup(ctx context.Context, options application.ServiceOptions) error {
	// Any initialization code here
	v.ctx = ctx
	return nil
}

// GetVolumes returns a list of volumes
func (v *VolumeControllerImpl) GetVolumes() ([]common.Volume, error) {
	if v.appService.GetContext() == nil {
		return nil, fmt.Errorf("not logged in")
	}
	return v.appService.GetContext().VolumeService.GetVolumes(v.ctx)
}

// IsMounted checks if a volume is mounted
func (v *VolumeControllerImpl) IsMounted(volume common.Volume) (bool, error) {
	if v.appService.GetContext() == nil {
		return false, fmt.Errorf("not logged in")
	}
	return v.appService.GetContext().MultiVolumeMounter.IsVolumeMounted(volume.Name), nil
}

// Mount mounts a volume
func (v *VolumeControllerImpl) Mount(volume common.Volume) error {
	if v.appService.GetContext() == nil {
		return fmt.Errorf("not logged in")
	}
	if !v.vfsMounted {
		if err := v.appService.GetContext().MultiVolumeMounter.Start(); err != nil {
			log.Log.Error("failed to start vfs", zap.Error(err))
			return err
		}
		v.vfsMounted = true
	}

	return v.appService.GetContext().MultiVolumeMounter.Mount(volume.Name)
}

// Unmount unmounts a volume
func (v *VolumeControllerImpl) Unmount(volume common.Volume) error {
	if v.appService.GetContext() == nil {
		return fmt.Errorf("not logged in")
	}
	return v.appService.GetContext().MultiVolumeMounter.Unmount(volume.Name)
}
