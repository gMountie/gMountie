package mount

import (
	"errors"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"

	grpcmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	protomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type NegotiateMaxWriteBytesSuite struct {
	suite.Suite
	client      *grpcmocks.MockClient
	versionMock *protomocks.MockVersionServiceClient
	cfg         *config.FUSEConfig
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

// TestLazyUnmount_NonExistentPath confirms that lazyUnmount surfaces
// the fusermount3 error (including its stderr) when the target path
// doesn't exist. The whole purpose of the helper is to give callers
// something actionable when the fallback itself fails; a swallowed
// error would defeat the point.
type LazyUnmountTestSuite struct{ suite.Suite }

func (s *LazyUnmountTestSuite) TestNonExistentPathReturnsWrappedError() {
	err := lazyUnmount("/nonexistent/path/that/cannot/possibly/be/mounted")
	s.Require().Error(err)
	// pkg/errors.Wrapf wraps the underlying exec error; the message
	// embeds the path and the fusermount3 stderr.
	s.Assert().Contains(err.Error(), "fusermount3 -uz")
	s.Assert().Contains(err.Error(), "/nonexistent/path/that/cannot/possibly/be/mounted")
}

func TestLazyUnmountSuite(t *testing.T) {
	suite.Run(t, new(LazyUnmountTestSuite))
}

// Make sure errors stays imported now that the suite uses it nowhere
// directly (suite already imports it via the package's other tests).
var _ = errors.New

// CreateMountOptionsSuite tests capability-bit wiring in createMountOptions.
type CreateMountOptionsSuite struct{ suite.Suite }

func (s *CreateMountOptionsSuite) baseCfg() *config.FUSEConfig {
	return &config.FUSEConfig{
		MaxWriteBytes:  config.DefaultFUSEMaxWriteBytes,
		MaxBackground:  config.DefaultFUSEMaxBackground,
		WritebackCache: false,
		AttrTimeout:    config.DefaultFUSEAttrTimeout,
		EntryTimeout:   config.DefaultFUSEEntryTimeout,
	}
}

func (s *CreateMountOptionsSuite) TestHandleKillPrivOnSetsCap() {
	cfg := s.baseCfg()
	cfg.HandleKillPriv = true
	opts := createMountOptions("127.0.0.1:9449", "vol", cfg, config.DefaultFUSEMaxWriteBytes)
	s.NotZero(opts.ExtraCapabilities&fuse.CAP_HANDLE_KILLPRIV_V2,
		"HandleKillPriv=true must set CAP_HANDLE_KILLPRIV_V2 in ExtraCapabilities")
}

func (s *CreateMountOptionsSuite) TestHandleKillPrivOffLeavesCapUnset() {
	cfg := s.baseCfg()
	cfg.HandleKillPriv = false
	opts := createMountOptions("127.0.0.1:9449", "vol", cfg, config.DefaultFUSEMaxWriteBytes)
	s.Zero(opts.ExtraCapabilities&fuse.CAP_HANDLE_KILLPRIV_V2,
		"HandleKillPriv=false must leave CAP_HANDLE_KILLPRIV_V2 unset")
}

func TestCreateMountOptionsSuite(t *testing.T) {
	suite.Run(t, new(CreateMountOptionsSuite))
}

// BuildFSOptionsSuite tests the pure buildFSOptions helper that assembles
// gofs.Options from a fuse.MountOptions and a FUSEConfig. Testing this
// function directly avoids needing a real FUSE mount while still verifying
// that AttrTimeout/EntryTimeout are wired through correctly.
type BuildFSOptionsSuite struct{ suite.Suite }

func (s *BuildFSOptionsSuite) TestAttrEntryTimeoutPropagated() {
	mountOpts := &fuse.MountOptions{Name: "test"}
	cfg := &config.FUSEConfig{
		MaxWriteBytes:  config.DefaultFUSEMaxWriteBytes,
		MaxBackground:  config.DefaultFUSEMaxBackground,
		WritebackCache: false,
		AttrTimeout:    config.DefaultFUSEAttrTimeout,
		EntryTimeout:   config.DefaultFUSEEntryTimeout,
	}
	opts := buildFSOptions(mountOpts, cfg)
	s.Require().NotNil(opts.AttrTimeout, "AttrTimeout pointer must be non-nil")
	s.Require().NotNil(opts.EntryTimeout, "EntryTimeout pointer must be non-nil")
	s.Equal(config.DefaultFUSEAttrTimeout, *opts.AttrTimeout)
	s.Equal(config.DefaultFUSEEntryTimeout, *opts.EntryTimeout)
}

// TestZeroValuedConfigGetsDefaults pins the landmine fix: a zero-valued
// FUSEConfig (struct-building library consumers, the e2e harness) must get
// the 1s production defaults, NOT non-nil zero durations — go-fuse treats
// a non-nil zero as kernel-cache-off, which amplifies every kernel op into
// a fresh GETATTR/LOOKUP round-trip and wedged the e2e fs suite.
func (s *BuildFSOptionsSuite) TestZeroValuedConfigGetsDefaults() {
	mountOpts := &fuse.MountOptions{Name: "test"}
	opts := buildFSOptions(mountOpts, &config.FUSEConfig{})
	s.Require().NotNil(opts.AttrTimeout)
	s.Require().NotNil(opts.EntryTimeout)
	s.Equal(config.DefaultFUSEAttrTimeout, *opts.AttrTimeout,
		"zero AttrTimeout must mean default, never cache-off")
	s.Equal(config.DefaultFUSEEntryTimeout, *opts.EntryTimeout,
		"zero EntryTimeout must mean default, never cache-off")
}

func (s *BuildFSOptionsSuite) TestCustomTimeoutsPropagated() {
	mountOpts := &fuse.MountOptions{Name: "test"}
	cfg := &config.FUSEConfig{
		MaxWriteBytes: config.DefaultFUSEMaxWriteBytes,
		MaxBackground: config.DefaultFUSEMaxBackground,
		AttrTimeout:   30 * time.Second,
		EntryTimeout:  15 * time.Second,
	}
	opts := buildFSOptions(mountOpts, cfg)
	s.Equal(30*time.Second, *opts.AttrTimeout)
	s.Equal(15*time.Second, *opts.EntryTimeout)
}

func (s *BuildFSOptionsSuite) TestMountOptionsPassedThrough() {
	mountOpts := &fuse.MountOptions{Name: "gMountie", MaxBackground: 64}
	cfg := defaultTestFUSEConfig()
	opts := buildFSOptions(mountOpts, cfg)
	s.Equal("gMountie", opts.Name)
	s.Equal(64, opts.MaxBackground)
}

func TestBuildFSOptionsSuite(t *testing.T) {
	suite.Run(t, new(BuildFSOptionsSuite))
}
