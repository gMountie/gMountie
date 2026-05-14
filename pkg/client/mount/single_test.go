package mount

import (
	"gmountie/pkg/proto"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gmountie/internal/mocks/pkg/client/grpc"
	mockProto "gmountie/internal/mocks/pkg/proto"

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
	s.mounter = NewSingleVolumeMounter(s.client)

	var err error
	s.tempDir, err = os.MkdirTemp("", "gmountie-test-*")
	s.Require().NoError(err)
	s.mntDir = filepath.Join(s.tempDir, "mnt")
	err = os.Mkdir(s.mntDir, 0755)
	s.Require().NoError(err)

	// Setup common mock expectations
	mockFsClient := &mockProto.MockRpcFsClient{}
	//mockFileClient := &mockProto.MockRpcFileClient{}
	//mockVolumeClient := &mockProto.MockVolumeServiceClient{}

	mockFsClient.EXPECT().Access(mock.Anything, mock.Anything).Return(&proto.AccessReply{
		Status: int32(fuse.ENOSYS),
	}, nil).Maybe()
	mockFsClient.EXPECT().GetAttr(mock.Anything, mock.Anything).Return(&proto.GetAttrReply{
		Status: int32(fuse.ENOSYS),
	}, nil).Maybe()

	s.client.EXPECT().Fs().Return(mockFsClient).Maybe()
	s.client.EXPECT().GetEndpoint().Return("localhost:8080").Maybe()
	// LocalFileSystem now calls MetaTimeout/IOTimeout on every RPC; the FUSE
	// kernel makes many incoming calls during mount/unmount so we permit any
	// number including zero (Maybe) at well-defined fast values.
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
	s.client.EXPECT().IOTimeout().Return(30 * time.Second).Maybe()
	s.client.EXPECT().SessionID().Return("test-session").Maybe()
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

func TestSingleVolumeMounterTestSuite(t *testing.T) {
	suite.Run(t, new(SingleVolumeMounterTestSuite))
}
