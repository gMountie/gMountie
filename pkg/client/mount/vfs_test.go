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

type VFSVolumeMounterTestSuite struct {
	suite.Suite
	mounter VFSVolumeMounter
	client  *grpc.MockClient
	tempDir string
	mntDir  string
}

func (s *VFSVolumeMounterTestSuite) SetupTest() {
	s.client = grpc.NewMockClient(s.T())

	var err error
	s.tempDir, err = os.MkdirTemp("", "gmountie-vfs-test-*")
	s.Require().NoError(err)
	s.mntDir = filepath.Join(s.tempDir, "mnt")
	err = os.Mkdir(s.mntDir, 0755)
	s.Require().NoError(err)

	s.mounter = NewMultiVolumeMounter(s.client, s.mntDir)

	// Setup common mock expectations
	mockFsClient := &mockProto.MockRpcFsClient{}
	mockFileClient := &mockProto.MockRpcFileClient{}

	mockFsClient.EXPECT().Access(mock.Anything, mock.Anything).Return(&proto.AccessReply{
		Status: int32(fuse.ENOSYS),
	}, nil).Maybe()
	mockFsClient.EXPECT().GetAttr(mock.Anything, mock.Anything).Return(&proto.GetAttrReply{
		Status: int32(fuse.ENOSYS),
	}, nil).Maybe()

	s.client.EXPECT().Fs().Return(mockFsClient).Maybe()
	s.client.EXPECT().File().Return(mockFileClient).Maybe()
	s.client.EXPECT().GetEndpoint().Return("localhost:8080").Maybe()
	// LocalFileSystem now calls MetaTimeout/IOTimeout on every RPC; the FUSE
	// kernel makes many incoming calls during mount/unmount so we permit any
	// number including zero (Maybe) at well-defined fast values.
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
	s.client.EXPECT().IOTimeout().Return(30 * time.Second).Maybe()
}

func (s *VFSVolumeMounterTestSuite) TearDownTest() {
	err := s.mounter.Close()
	s.Require().NoError(err)
	s.Require().NoError(os.RemoveAll(s.tempDir))
}

func (s *VFSVolumeMounterTestSuite) TestStart() {
	// Test starting the mounter
	err := s.mounter.Start()
	s.Require().NoError(err)

	// Verify the mount directory exists and is accessible
	_, err = os.Stat(s.mntDir)
	s.Assert().NoError(err)
}

func (s *VFSVolumeMounterTestSuite) TestMountBeforeStart() {
	// Test mounting before starting should fail
	err := s.mounter.Mount("test-volume")
	s.Assert().Error(err)
	s.Assert().Contains(err.Error(), "mounter not started")
}

func (s *VFSVolumeMounterTestSuite) TestMountAndUnmount() {
	// Start the mounter first
	err := s.mounter.Start()
	s.Require().NoError(err)

	// Test mounting a volume
	err = s.mounter.Mount("test-volume")
	s.Require().NoError(err)

	// Verify volume is mounted
	mounted := s.mounter.IsVolumeMounted("test-volume")
	s.Assert().True(mounted)

	// Test unmounting the volume
	err = s.mounter.Unmount("test-volume")
	s.Require().NoError(err)

	// Verify volume is unmounted
	mounted = s.mounter.IsVolumeMounted("test-volume")
	s.Assert().False(mounted)
}

func (s *VFSVolumeMounterTestSuite) TestMountDuplicate() {
	// Start the mounter
	err := s.mounter.Start()
	s.Require().NoError(err)

	// Mount volume first time
	err = s.mounter.Mount("test-volume")
	s.Require().NoError(err)

	// Attempt to mount same volume again
	err = s.mounter.Mount("test-volume")
	s.Assert().Error(err)
	s.Assert().Contains(err.Error(), "already mounted")
}

func (s *VFSVolumeMounterTestSuite) TestUnmountNonExistent() {
	// Start the mounter
	err := s.mounter.Start()
	s.Require().NoError(err)

	// Test unmounting a volume that isn't mounted
	err = s.mounter.Unmount("non-existent-volume")
	s.Assert().Error(err)
	s.Assert().Contains(err.Error(), "not mounted")
}

func (s *VFSVolumeMounterTestSuite) TestUnmountAll() {
	// Start the mounter
	err := s.mounter.Start()
	s.Require().NoError(err)

	// Mount multiple volumes
	err = s.mounter.Mount("test-volume-1")
	s.Require().NoError(err)
	err = s.mounter.Mount("test-volume-2")
	s.Require().NoError(err)

	// Verify volumes are mounted
	s.Assert().True(s.mounter.IsVolumeMounted("test-volume-1"))
	s.Assert().True(s.mounter.IsVolumeMounted("test-volume-2"))

	// Test unmounting all
	err = s.mounter.UnmountAll()
	s.Require().NoError(err)

	// Verify all volumes are unmounted
	s.Assert().False(s.mounter.IsVolumeMounted("test-volume-1"))
	s.Assert().False(s.mounter.IsVolumeMounted("test-volume-2"))
}

func (s *VFSVolumeMounterTestSuite) TestClose() {
	// Start the mounter
	err := s.mounter.Start()
	s.Require().NoError(err)

	// Mount a volume
	err = s.mounter.Mount("test-volume")
	s.Require().NoError(err)

	// Test closing
	err = s.mounter.Close()
	s.Require().NoError(err)

	// Verify mount point is no longer accessible
	_, err = os.Stat(filepath.Join(s.mntDir, "test-volume"))
	s.Assert().Error(err)
}

func (s *VFSVolumeMounterTestSuite) TestCloseWithoutStart() {
	// Test closing without starting
	err := s.mounter.Close()
	s.Assert().NoError(err)
}

func TestVFSVolumeMounterTestSuite(t *testing.T) {
	suite.Run(t, new(VFSVolumeMounterTestSuite))
}
