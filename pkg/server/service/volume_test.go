package service

import (
	"gmountie/pkg/server/config"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/suite"
)

type VolumeServiceTestSuite struct {
	suite.Suite
	service VolumeService
	config  *config.Config
}

func (s *VolumeServiceTestSuite) SetupTest() {
	s.config = &config.Config{
		Volumes: []*config.VolumeConfig{
			{
				Name: "volume1",
				Path: s.T().TempDir(),
			},
			{
				Name: "volume2",
				Path: s.T().TempDir(),
			},
		},
	}
	svc, err := NewVolumeService(s.config)
	s.Require().NoError(err)
	s.service = svc
}

func (s *VolumeServiceTestSuite) TestList() {
	// Test
	volumes, err := s.service.List()

	// Verify
	s.Require().NoError(err)
	s.Assert().Len(volumes, 2)
	s.Assert().Equal("volume1", volumes[0].Name)
	s.Assert().Equal("volume2", volumes[1].Name)
}

func (s *VolumeServiceTestSuite) TestGetVolumeFileSystem_Success() {
	// Test
	fs, err := s.service.GetVolumeFileSystem("volume1")

	// Verify
	s.Require().NoError(err)
	s.Assert().NotNil(fs)
	s.Assert().Implements((*pathfs.FileSystem)(nil), fs)
}

func (s *VolumeServiceTestSuite) TestGetVolumeFileSystem_NotFound() {
	// Test
	fs, err := s.service.GetVolumeFileSystem("non-existent")

	// Verify
	s.Require().Error(err)
	s.Assert().Nil(fs)
	s.Assert().Contains(err.Error(), "volume non-existent not found")
}

func (s *VolumeServiceTestSuite) TestNewVolumeService_WithMiddleware() {
	// Setup
	middlewareCalled := false
	testMiddleware := func(fs pathfs.FileSystem) pathfs.FileSystem {
		middlewareCalled = true
		return fs
	}

	// Test
	service, err := NewVolumeService(s.config, WithMiddleware(testMiddleware))

	// Verify
	s.Require().NoError(err)
	s.Assert().NotNil(service)
	s.Assert().True(middlewareCalled)
}

func (s *VolumeServiceTestSuite) TestNewVolumeService_EmptyVolumes() {
	// Setup
	emptyConfig := &config.Config{
		Volumes: []*config.VolumeConfig{},
	}

	// Test
	service, err := NewVolumeService(emptyConfig)
	s.Require().NoError(err)
	volumes, err := service.List()

	// Verify
	s.Require().NoError(err)
	s.Assert().Empty(volumes)
}

func (s *VolumeServiceTestSuite) TestNewVolumeService_MultipleMiddleware() {
	// Setup
	middlewareCalls := 0
	testMiddleware := func(fs pathfs.FileSystem) pathfs.FileSystem {
		middlewareCalls++
		return fs
	}

	// Test
	service, err := NewVolumeService(s.config,
		WithMiddleware(testMiddleware),
		WithMiddleware(testMiddleware))

	// Verify
	s.Require().NoError(err)
	s.Assert().NotNil(service)
	s.Assert().Equal(2*len(s.config.Volumes), middlewareCalls)
}

func (s *VolumeServiceTestSuite) TestVolumeService_VolumeListConsistency() {
	// Test initial state
	volumes, err := s.service.List()
	s.Require().NoError(err)
	initialCount := len(volumes)

	// Get filesystems for all volumes
	for _, vol := range volumes {
		fs, err := s.service.GetVolumeFileSystem(vol.Name)
		s.Require().NoError(err)
		s.Assert().NotNil(fs)
	}

	// Verify list remains unchanged
	volumesAfter, err := s.service.List()
	s.Require().NoError(err)
	s.Assert().Len(volumesAfter, initialCount)
	s.Assert().Equal(volumes, volumesAfter)
}

func TestVolumeServiceTestSuite(t *testing.T) {
	suite.Run(t, new(VolumeServiceTestSuite))
}
