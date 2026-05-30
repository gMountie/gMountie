package controller

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"google.golang.org/grpc"
)

// VolumeServiceImpl implements the VolumeService gRPC service
type VolumeServiceImpl struct {
	service service.VolumeService
	proto.UnimplementedVolumeServiceServer
}

// NewVolumeService creates a new VolumeServiceImpl
func NewVolumeService(service service.VolumeService) *VolumeServiceImpl {
	return &VolumeServiceImpl{
		service: service,
	}
}

// Register registers the gRPC server
func (v *VolumeServiceImpl) Register(server *grpc.Server) {
	proto.RegisterVolumeServiceServer(server, v)
}

// List lists all volumes
func (v *VolumeServiceImpl) List(ctx context.Context, _ *proto.VolumeListRequest) (*proto.VolumeListReply, error) {
	// Find volumes
	volumes, err := v.service.List(ctx)
	if err != nil {
		return nil, err
	}
	// Convert volumes to gRPC reply
	volumesReply := make([]*proto.Volume, 0, len(volumes))
	for _, volume := range volumes {
		volumesReply = append(volumesReply, &proto.Volume{
			Name: volume.Name,
		})
	}

	reply := &proto.VolumeListReply{
		Volumes: volumesReply,
	}
	return reply, nil
}

// Resolve tells the client where to reach a volume. An empty location means the
// volume is served here (the OSS default); a non-empty location is a referral.
func (v *VolumeServiceImpl) Resolve(ctx context.Context, req *proto.VolumeResolveRequest) (*proto.VolumeResolveReply, error) {
	location, err := v.service.Resolve(ctx, req.GetName())
	if err != nil {
		return nil, err
	}
	return &proto.VolumeResolveReply{Location: location}, nil
}
