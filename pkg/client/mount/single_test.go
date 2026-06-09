package mount

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"

	"go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type SingleVolumeMounterTestSuite struct {
	suite.Suite
	mounter SingleVolumeMounter
	client  *grpc.MockClient
	tempDir string
	mntDir  string
}

func (s *SingleVolumeMounterTestSuite) SetupTest() {
	s.client = grpc.NewMockClient(s.T())
	s.mounter = NewSingleVolumeMounter(s.client, &config.FUSEConfig{
		MaxWriteBytes:  config.DefaultFUSEMaxWriteBytes,
		MaxBackground:  config.DefaultFUSEMaxBackground,
		WritebackCache: config.DefaultFUSEWritebackCache,
	}, defaultTestCacheConfig(), false)

	var err error
	s.tempDir, err = os.MkdirTemp("", "gmountie-test-*")
	s.Require().NoError(err)
	s.mntDir = filepath.Join(s.tempDir, "mnt")
	err = os.Mkdir(s.mntDir, 0755)
	s.Require().NoError(err)

	// Setup common mock expectations
	mockFsClient := &mockProto.MockRpcFsClient{}
	mockVersionClient := &mockProto.MockVersionServiceClient{}
	//mockFileClient := &mockProto.MockRpcFileClient{}
	//mockVolumeClient := &mockProto.MockVolumeServiceClient{}

	mockFsClient.EXPECT().Access(mock.Anything, mock.Anything).Return(&proto.AccessReply{
		Status: int32(fuse.ENOSYS),
	}, nil).Maybe()
	mockFsClient.EXPECT().GetAttr(mock.Anything, mock.Anything).Return(&proto.GetAttrReply{
		Status: int32(fuse.ENOSYS),
	}, nil).Maybe()
	// Mount path negotiates the FUSE frame ceiling via Version.Get; return
	// the default frame size so the configured MaxWriteBytes is preserved.
	mockVersionClient.EXPECT().Get(mock.Anything, mock.Anything).Return(&proto.VersionReply{
		FrameSizeBytes: int32(config.DefaultFUSEMaxWriteBytes),
	}, nil).Maybe()

	s.client.EXPECT().Fs().Return(mockFsClient).Maybe()
	s.client.EXPECT().Version().Return(mockVersionClient).Maybe()
	s.client.EXPECT().GetEndpoint().Return("localhost:8080").Maybe()
	// LocalFileSystem now calls MetaTimeout/IOTimeout on every RPC; the FUSE
	// kernel makes many incoming calls during mount/unmount so we permit any
	// number including zero (Maybe) at well-defined fast values.
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
	s.client.EXPECT().IOTimeout().Return(30 * time.Second).Maybe()
	s.client.EXPECT().SessionID().Return("test-session").Maybe()
	s.client.EXPECT().Lifetime().Return(context.Background()).Maybe()
	// WhoAmI is called once per Mount when raw_ids=false. We allow it to fail
	// (degrade path) so existing tests keep passing without extra per-test setup.
	s.client.EXPECT().WhoAmI(mock.Anything, mock.Anything).
		Return(nil, errors.New("test: degrade to raw ids")).Maybe()
}

func (s *SingleVolumeMounterTestSuite) TearDownTest() {
	// First unmount all volumes
	err := s.mounter.Close()
	s.Require().NoError(err)
	s.Require().NoError(os.RemoveAll(s.tempDir))
}

func (s *SingleVolumeMounterTestSuite) TestMount() {
	err := s.mounter.Mount("test-volume", s.mntDir)
	s.Require().NoError(err)

	// Verify volume is mounted
	mounted := s.mounter.IsVolumeMounted("test-volume")
	s.Assert().True(mounted)
}

func (s *SingleVolumeMounterTestSuite) TestMountDuplicate() {
	// Mount volume first time
	err := s.mounter.Mount("test-volume", s.mntDir)
	s.Require().NoError(err)

	// Attempt to mount same volume again
	err = s.mounter.Mount("test-volume", s.mntDir)
	s.Assert().Error(err)
}

func (s *SingleVolumeMounterTestSuite) TestUnmount() {
	// Setup - mount a volume first
	err := s.mounter.Mount("test-volume", s.mntDir)
	s.Require().NoError(err)

	// Test unmounting
	err = s.mounter.Unmount("test-volume")
	s.Require().NoError(err)

	// Verify volume is unmounted
	mounted := s.mounter.IsVolumeMounted("test-volume")
	s.Assert().False(mounted)
}

func (s *SingleVolumeMounterTestSuite) TestUnmountNonExistent() {
	// Test unmounting a volume that isn't mounted
	err := s.mounter.Unmount("non-existent-volume")
	s.Assert().Error(err)
}

func (s *SingleVolumeMounterTestSuite) TestUnmountAll() {
	// Setup - mount multiple volumes
	mnt2 := filepath.Join(s.tempDir, "mnt2")
	err := os.Mkdir(mnt2, 0755)
	s.Require().NoError(err)
	err = s.mounter.Mount("test-volume-1", s.mntDir)
	s.Require().NoError(err)
	err = s.mounter.Mount("test-volume-2", mnt2)
	s.Require().NoError(err)

	// Test unmounting all
	err = s.mounter.UnmountAll()
	s.Require().NoError(err)

	// Verify all volumes are unmounted
	mounted1 := s.mounter.IsVolumeMounted("test-volume-1")
	mounted2 := s.mounter.IsVolumeMounted("test-volume-2")
	s.Assert().False(mounted1)
	s.Assert().False(mounted2)
}

func (s *SingleVolumeMounterTestSuite) TestClose() {
	// Setup - mount a volume
	err := s.mounter.Mount("test-volume", s.mntDir)
	s.Require().NoError(err)

	// Test closing
	err = s.mounter.Close()
	s.Require().NoError(err)

	// Verify volume is unmounted
	mounted := s.mounter.IsVolumeMounted("test-volume")
	s.Assert().False(mounted)
}

func (s *SingleVolumeMounterTestSuite) TestMountFailsWithLockedCacheDir() {
	// Pre-acquire the lock on the volume-scoped cache dir before Mount runs.
	cacheRoot := s.T().TempDir()
	volume := "test-vol"
	stub, err := persist.Open(persist.Options{Root: filepath.Join(cacheRoot, volume)})
	s.Require().NoError(err)
	defer stub.Close()

	cacheCfg := config.CacheConfig{
		Enabled:        true,
		Path:           cacheRoot,
		MemoryMaxBytes: config.DefaultCacheMemoryMaxBytes,
		DiskMaxBytes:   config.DefaultCacheDiskMaxBytes,
		ChunkSizeBytes: config.DefaultCacheChunkSizeBytes,
		AttrTTL:        time.Second,
		DirTTL:         time.Second,
		NegativeTTL:    time.Second,
	}
	m := NewSingleVolumeMounter(s.client, &config.FUSEConfig{
		MaxWriteBytes:  config.DefaultFUSEMaxWriteBytes,
		MaxBackground:  config.DefaultFUSEMaxBackground,
		WritebackCache: config.DefaultFUSEWritebackCache,
	}, cacheCfg, false)
	err = m.Mount(volume, s.T().TempDir())
	s.Require().Error(err)
	s.Assert().ErrorIs(err, persist.ErrCacheLocked)
}

func TestSingleVolumeMounterTestSuite(t *testing.T) {
	suite.Run(t, new(SingleVolumeMounterTestSuite))
}
