package service

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// VolumeService is an interface that provides information about the volumes
type VolumeService interface {
	GetVolumes(ctx context.Context) ([]common.Volume, error)
}

// VolumeServiceImpl is a service that provides information about the volumes
type VolumeServiceImpl struct {
	client grpc.Client
}

// NewVolumeService creates a new VolumeServiceImpl
func NewVolumeService(client grpc.Client) VolumeService {
	return &VolumeServiceImpl{
		client: client,
	}
}

// GetVolumes returns a list of volumes
func (v *VolumeServiceImpl) GetVolumes(ctx context.Context) ([]common.Volume, error) {
	reply, err := v.client.Volume().List(ctx, &proto.VolumeListRequest{})
	if err != nil {
		return nil, err
	}
	volumes := make([]common.Volume, 0)
	for _, v := range reply.Volumes {
		volumes = append(volumes, common.Volume{Name: v.Name})
	}
	return volumes, nil
}
