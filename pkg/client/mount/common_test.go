package mount

import (
	"errors"
	"testing"

	"gmountie/pkg/client/config"
	"gmountie/pkg/proto"

	grpcmocks "gmountie/internal/mocks/pkg/client/grpc"
	protomocks "gmountie/internal/mocks/pkg/proto"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type NegotiateMaxWriteBytesSuite struct {
	suite.Suite
	client       *grpcmocks.MockClient
	versionMock  *protomocks.MockVersionServiceClient
	cfg          *config.FUSEConfig
}

func (s *NegotiateMaxWriteBytesSuite) SetupTest() {
	s.client = grpcmocks.NewMockClient(s.T())
	s.versionMock = protomocks.NewMockVersionServiceClient(s.T())
	s.client.EXPECT().Version().Return(s.versionMock).Once()
	s.cfg = &config.FUSEConfig{
		MaxWriteBytes:  1 << 20, // 1 MiB
		MaxBackground:  config.DefaultFUSEMaxBackground,
		WritebackCache: false,
	}
}

func (s *NegotiateMaxWriteBytesSuite) TestErrorFallsBackToConfigured() {
	s.versionMock.EXPECT().
		Get(mock.Anything, mock.Anything).
		Return(nil, errors.New("transient network failure")).Once()

	got := negotiateMaxWriteBytes(s.client, s.cfg)
	s.Equal(s.cfg.MaxWriteBytes, got, "Version error must fall back to configured MaxWriteBytes")
}

func (s *NegotiateMaxWriteBytesSuite) TestZeroFrameFallsBackToConfigured() {
	// Old / unimplemented server: FrameSizeBytes left at zero.
	s.versionMock.EXPECT().
		Get(mock.Anything, mock.Anything).
		Return(&proto.VersionReply{FrameSizeBytes: 0}, nil).Once()

	got := negotiateMaxWriteBytes(s.client, s.cfg)
	s.Equal(s.cfg.MaxWriteBytes, got, "server reporting zero frame size must fall back to configured")
}

func (s *NegotiateMaxWriteBytesSuite) TestServerCapsBelowConfigured() {
	// Server frame ceiling (512 KiB) below configured (1 MiB) — cap down.
	const serverFrame = 512 << 10
	s.versionMock.EXPECT().
		Get(mock.Anything, mock.Anything).
		Return(&proto.VersionReply{FrameSizeBytes: int32(serverFrame)}, nil).Once()

	got := negotiateMaxWriteBytes(s.client, s.cfg)
	s.Equal(serverFrame, got, "configured 1 MiB must be capped at server's 512 KiB")
}

func (s *NegotiateMaxWriteBytesSuite) TestServerAtOrAboveConfiguredKeepsConfigured() {
	// Server frame ceiling (2 MiB) above configured (1 MiB) — no cap.
	s.versionMock.EXPECT().
		Get(mock.Anything, mock.Anything).
		Return(&proto.VersionReply{FrameSizeBytes: int32(2 << 20)}, nil).Once()

	got := negotiateMaxWriteBytes(s.client, s.cfg)
	s.Equal(s.cfg.MaxWriteBytes, got, "server frame at or above configured must keep configured value")
}

func TestNegotiateMaxWriteBytesSuite(t *testing.T) {
	suite.Run(t, new(NegotiateMaxWriteBytesSuite))
}
