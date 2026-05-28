package api

import (
	"context"
	"gmountie/pkg/server/principal"
	"gmountie/test/e2e/utils"
	"testing"

	"github.com/stretchr/testify/suite"
)

type VolumeAPITestSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
}

func (s *VolumeAPITestSuite) SetupSuite() {
	// Create a new auth service.
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(true),
	)
	if err != nil {
		s.T().Fatal(err)
	}
	err = testAppCtx.Start()
	if err != nil {
		s.T().Fatal(err)
	}
	s.testAppCtx = testAppCtx
}

func (s *VolumeAPITestSuite) TestListFiles() {
	clientVolumes, err := s.testAppCtx.GetClientApp().VolumeService.GetVolumes(context.Background())
	s.Require().NoError(err)
	// VolumeService.List filters by the authenticated principal (Phase 7 ACL).
	// The client dials authenticated as "test", so the server-side view we
	// compare against must be resolved for that same principal — a bare
	// context.Background() carries no principal and (correctly) yields no
	// volumes under the fail-closed ACL.
	ctx := principal.WithPrincipal(context.Background(), "test")
	serverVolumes, err := s.testAppCtx.GetServerApp().VolumeService.List(ctx)

	s.Require().NoError(err)
	s.Assert().Equal(clientVolumes, serverVolumes)
}

func (s *VolumeAPITestSuite) TearDownSuite() {
	err := s.testAppCtx.Close()
	if err != nil {
		s.T().Fatal(err)
	}
}

func TestVolumeAPITestSuite(t *testing.T) {
	suite.Run(t, new(VolumeAPITestSuite))
}
