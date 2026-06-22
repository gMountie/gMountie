package mount

import (
	"testing"
	"time"

	grpcmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	protomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type MountParamsSuite struct {
	suite.Suite
	client *grpcmocks.MockClient
}

func (s *MountParamsSuite) SetupTest() {
	s.client = grpcmocks.NewMockClient(s.T())
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
}

// TestRawIDsSkipsWhoAmI verifies that rawIDs=true runs Version negotiation but
// skips WhoAmI, returns the configured MaxWriteBytes, DefaultPermissions=false,
// and a nil rewriter.
func (s *MountParamsSuite) TestRawIDsSkipsWhoAmI() {
	ver := protomocks.NewMockVersionServiceClient(s.T())
	ver.EXPECT().Get(mock.Anything, mock.Anything).
		Return(&proto.VersionReply{FrameSizeBytes: 0}, nil).Once()
	s.client.EXPECT().Version().Return(ver).Once()

	params, rewriter := negotiateMountParams(s.client, &config.FUSEConfig{MaxWriteBytes: 1 << 20}, true, "vol")
	s.Equal(1<<20, params.MaxWriteBytes)
	s.False(params.DefaultPermissions)
	s.Nil(rewriter)
}

// TestSquashModeReturnsRewriterAndDefaultPermissions verifies that when
// rawIDs=false and the server responds with mapping_mode="squash", the function
// returns DefaultPermissions=true and a non-nil rewriter.
func (s *MountParamsSuite) TestSquashModeReturnsRewriterAndDefaultPermissions() {
	ver := protomocks.NewMockVersionServiceClient(s.T())
	ver.EXPECT().Get(mock.Anything, mock.Anything).
		Return(&proto.VersionReply{FrameSizeBytes: 0}, nil).Once()
	s.client.EXPECT().Version().Return(ver).Once()

	identity := &proto.Identity{
		Uid:        1000,
		PrimaryGid: 1000,
		Gids:       []uint32{1000},
		MappingMode: mappingModeSquash,
	}
	s.client.EXPECT().WhoAmI(mock.Anything, "vol").Return(identity, nil).Once()

	params, rewriter := negotiateMountParams(s.client, &config.FUSEConfig{MaxWriteBytes: 1 << 20}, false, "vol")
	s.Equal(1<<20, params.MaxWriteBytes)
	s.True(params.DefaultPermissions)
	s.NotNil(rewriter)
}

func TestMountParamsSuite(t *testing.T) {
	suite.Run(t, new(MountParamsSuite))
}
