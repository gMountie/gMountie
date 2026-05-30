package controller

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/internal/mocks/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type VolumeServiceTestSuite struct {
	suite.Suite
	server  *VolumeServiceImpl
	service *service.MockVolumeService
}

func (s *VolumeServiceTestSuite) SetupTest() {
	s.service = new(service.MockVolumeService)
	s.server = NewVolumeService(s.service)
}

func (s *VolumeServiceTestSuite) TestList_Success() {
	// Setup
	expectedVolumes := []common.Volume{
		{Name: "volume1"},
		{Name: "volume2"},
	}
	s.service.On("List", mock.Anything).Return(expectedVolumes, nil)

	// Test
	reply, err := s.server.List(context.Background(), &proto.VolumeListRequest{})

	// Verify
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Len(reply.Volumes, 2)
	s.Assert().Equal("volume1", reply.Volumes[0].Name)
	s.Assert().Equal("volume2", reply.Volumes[1].Name)
	s.service.AssertExpectations(s.T())
}

func (s *VolumeServiceTestSuite) TestList_EmptyList() {
	// Setup
	s.service.On("List", mock.Anything).Return([]common.Volume{}, nil)

	// Test
	reply, err := s.server.List(context.Background(), &proto.VolumeListRequest{})

	// Verify
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Empty(reply.Volumes)
	s.service.AssertExpectations(s.T())
}

func (s *VolumeServiceTestSuite) TestList_ServiceError() {
	// Setup
	expectedError := errors.New("test")
	s.service.On("List", mock.Anything).Return(nil, expectedError)

	// Test
	reply, err := s.server.List(context.Background(), &proto.VolumeListRequest{})

	// Verify
	s.Require().Error(err)
	s.Assert().Nil(reply)
	s.service.AssertExpectations(s.T())
}

func (s *VolumeServiceTestSuite) TestResolve_LocalLocation() {
	// An empty location means "served here" — the OSS default.
	s.service.On("Resolve", mock.Anything, "photos").Return("", nil)

	reply, err := s.server.Resolve(context.Background(), &proto.VolumeResolveRequest{Name: "photos"})

	s.Require().NoError(err)
	s.Require().NotNil(reply)
	s.Assert().Empty(reply.GetLocation())
	s.service.AssertExpectations(s.T())
}

func (s *VolumeServiceTestSuite) TestResolve_ReferralLocation() {
	// A non-empty location is a referral the client should reconnect to.
	s.service.On("Resolve", mock.Anything, "photos").Return("v-abc.data.example.com:443", nil)

	reply, err := s.server.Resolve(context.Background(), &proto.VolumeResolveRequest{Name: "photos"})

	s.Require().NoError(err)
	s.Require().NotNil(reply)
	s.Assert().Equal("v-abc.data.example.com:443", reply.GetLocation())
	s.service.AssertExpectations(s.T())
}

func (s *VolumeServiceTestSuite) TestResolve_ServiceError() {
	expectedError := errors.New("test")
	s.service.On("Resolve", mock.Anything, "photos").Return("", expectedError)

	reply, err := s.server.Resolve(context.Background(), &proto.VolumeResolveRequest{Name: "photos"})

	s.Require().Error(err)
	s.Assert().Nil(reply)
	s.service.AssertExpectations(s.T())
}

func TestVolumeServiceTestSuite(t *testing.T) {
	suite.Run(t, new(VolumeServiceTestSuite))
}
