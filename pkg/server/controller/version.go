package controller

import (
	"context"

	"gmountie/pkg"
	"gmountie/pkg/proto"

	"google.golang.org/grpc"
)

// VersionController is a thin passthrough over pkg.GetBuildInfo. Per the
// layering-service-features skill: this is the documented "no service
// layer for pure passthrough" exception — handler → io (well, package
// variable) with no business logic.
type VersionController struct {
	proto.UnimplementedVersionServiceServer
}

var _ proto.VersionServiceServer = (*VersionController)(nil)

func NewVersionController() *VersionController { return &VersionController{} }

func (c *VersionController) Register(s *grpc.Server) {
	proto.RegisterVersionServiceServer(s, c)
}

func (c *VersionController) Get(_ context.Context, _ *proto.VersionRequest) (*proto.VersionReply, error) {
	bi := pkg.GetBuildInfo()
	return &proto.VersionReply{Version: bi.Version, Commit: bi.Commit, Date: bi.Date}, nil
}
