package service

import (
	"context"
	"testing"

	"gmountie/pkg/proto"
	"gmountie/pkg/server/config"
	"gmountie/pkg/server/principal"

	"github.com/stretchr/testify/suite"
)

type BindIdentitySuite struct{ suite.Suite }

func TestBindIdentitySuite(t *testing.T) { suite.Run(t, new(BindIdentitySuite)) }

// serviceForVolume builds a VolumeService with a single volume "v" using the
// given mapping. Path is a real temp dir so the loopback constructs cleanly.
func (s *BindIdentitySuite) serviceForVolume(m config.MappingConfig) *VolumeServiceImpl {
	cfg := &config.Config{Volumes: []*config.VolumeConfig{{Name: "v", Path: s.T().TempDir(), Mapping: m}}}
	return NewVolumeService(cfg).(*VolumeServiceImpl)
}

func (s *BindIdentitySuite) TestSquashIgnoresPrincipalAndCaller() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	ctx := principal.WithPrincipal(context.Background(), "alice")
	id, err := svc.resolveIdentity(ctx, "v", &proto.Caller{Owner: &proto.Owner{Uid: 4242}})
	s.Require().NoError(err)
	s.Equal(uint32(1000), id.Uid)
}

func (s *BindIdentitySuite) TestStaticUsesCtxPrincipal() {
	m := config.MappingConfig{Mode: config.MappingModeStatic,
		Users: map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001}}}
	svc := s.serviceForVolume(m)
	id, err := svc.resolveIdentity(principal.WithPrincipal(context.Background(), "alice"), "v", nil)
	s.Require().NoError(err)
	s.Equal(uint32(1001), id.Uid)
}

func (s *BindIdentitySuite) TestStaticNoPrincipalFailsClosed() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeStatic,
		Users: map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001}}})
	_, err := svc.resolveIdentity(context.Background(), "v", nil)
	s.Require().Error(err)
}

func (s *BindIdentitySuite) TestPassthroughRootSquashDefaultOn() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModePassthrough, AnonUid: 65534})
	id, err := svc.resolveIdentity(context.Background(), "v", &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}})
	s.Require().NoError(err)
	s.Equal(uint32(65534), id.Uid)
}

func (s *BindIdentitySuite) TestPassthroughNoRootSquashKeepsRoot() {
	no := false
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModePassthrough, RootSquash: &no})
	id, err := svc.resolveIdentity(context.Background(), "v", &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}})
	s.Require().NoError(err)
	s.Equal(uint32(0), id.Uid)
}

func (s *BindIdentitySuite) TestBindIdentityReturnsFS() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	fs, err := svc.BindIdentity(context.Background(), "v", nil)
	s.Require().NoError(err)
	s.NotNil(fs)
}
