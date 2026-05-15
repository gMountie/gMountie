package controller

import (
	"context"

	"gmountie/pkg"
	"gmountie/pkg/proto"

	"google.golang.org/grpc"
)

// VersionController is a thin passthrough over pkg.GetBuildInfo plus a
// single config-derived value (frame_size_bytes) used by the client to
// negotiate FUSE MaxWrite. Per the layering-service-features skill: this
// is the documented "no service layer for pure passthrough" exception —
// handler → io (well, package variable + injected scalar) with no business
// logic.
type VersionController struct {
	proto.UnimplementedVersionServiceServer
	// frameSizeBytes is the server's configured streaming-frame ceiling.
	// Surfaced in VersionReply so the client can cap FUSE MaxWrite at the
	// negotiated value.
	frameSizeBytes int32
}

var _ proto.VersionServiceServer = (*VersionController)(nil)

// NewVersionController builds a VersionController. frameSizeBytes should
// be the server's ServerConfig.FrameSizeBytes; it's reported verbatim in
// VersionReply so clients can avoid asking the kernel for frames the
// server would split anyway.
func NewVersionController(frameSizeBytes int) *VersionController {
	return &VersionController{frameSizeBytes: int32(frameSizeBytes)}
}

func (c *VersionController) Register(s *grpc.Server) {
	proto.RegisterVersionServiceServer(s, c)
}

func (c *VersionController) Get(_ context.Context, _ *proto.VersionRequest) (*proto.VersionReply, error) {
	bi := pkg.GetBuildInfo()
	return &proto.VersionReply{
		Version:        bi.Version,
		Commit:         bi.Commit,
		Date:           bi.Date,
		FrameSizeBytes: c.frameSizeBytes,
	}, nil
}
