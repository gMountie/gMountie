package service

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ResolveSuite struct{ suite.Suite }

func TestResolveSuite(t *testing.T) { suite.Run(t, new(ResolveSuite)) }

// With default_allow (nil → true) and no per-user restriction, any principal
// resolves an existing volume to the local ("") location.
func (s *ResolveSuite) TestResolve_LocalForAccessibleVolume() {
	svc := buildService(s.T(), authWithUsers(nil))
	loc, err := svc.Resolve(ctxFor("alice"), "photos")
	s.Require().NoError(err)
	s.Empty(loc) // empty = served here
}

// A principal restricted away from the volume is denied (ACL runs first, so
// existence is not leaked).
func (s *ResolveSuite) TestResolve_DeniedPrincipal() {
	// bob may access only "team"; default_allow=false.
	svc := buildService(s.T(), authWithUsers(boolPtr(false),
		config.BasicAuthConfigUser{Username: "bob", Volumes: []string{"team"}},
	))
	_, err := svc.Resolve(ctxFor("bob"), "photos")
	s.Require().Error(err)
	s.Equal(codes.PermissionDenied, status.Code(err))
}

// An allowed principal asking for a volume that does not exist gets NotFound.
func (s *ResolveSuite) TestResolve_UnknownVolume() {
	svc := buildService(s.T(), authWithUsers(nil))
	_, err := svc.Resolve(ctxFor("alice"), "does-not-exist")
	s.Require().Error(err)
	s.Equal(codes.NotFound, status.Code(err))
}

// No authenticated principal is denied (mirrors PrincipalCanAccess).
func (s *ResolveSuite) TestResolve_NoPrincipal_Denied() {
	svc := buildService(s.T(), authWithUsers(boolPtr(false)))
	_, err := svc.Resolve(context.Background(), "photos")
	s.Require().Error(err)
	s.Equal(codes.PermissionDenied, status.Code(err))
}
