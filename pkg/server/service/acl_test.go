package service

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/principal"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ACLSuite struct{ suite.Suite }

func TestACLSuite(t *testing.T) { suite.Run(t, new(ACLSuite)) }

// buildService constructs a VolumeService with two volumes (photos, team) and
// the given auth config. Volumes use TempDirs so the loopback constructs cleanly.
func buildService(t *testing.T, auth config.AuthConfig) *VolumeServiceImpl {
	t.Helper()
	cfg := &config.Config{
		Volumes: []*config.VolumeConfig{
			{
				Name:    "photos",
				Path:    t.TempDir(),
				Mapping: config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000},
			},
			{
				Name:    "team",
				Path:    t.TempDir(),
				Mapping: config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000},
			},
		},
		Auth: auth,
	}
	svc, err := NewVolumeService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return svc.(*VolumeServiceImpl)
}

// ctxFor returns a context carrying the given principal name.
func ctxFor(name string) context.Context {
	return principal.WithPrincipal(context.Background(), name)
}

// authWithUsers builds a BasicAuthConfig from users and an optional
// default_allow setting.
func authWithUsers(defaultAllow *bool, users ...config.BasicAuthConfigUser) *config.BasicAuthConfig {
	return &config.BasicAuthConfig{
		AuthConfigBase: config.AuthConfigBase{Type: config.AuthConfigTypeBasic},
		Users:          users,
		DefaultAllow:   defaultAllow,
	}
}

func boolPtr(b bool) *bool { return &b }

// ── Tests: default_allow nil (→ true) ───────────────────────────────────────

func (s *ACLSuite) TestDefaultAllow_AliceCanAccessPhotos() {
	alice := config.BasicAuthConfigUser{Username: "alice", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder", Volumes: []string{"photos", "team"}}
	svc := buildService(s.T(), authWithUsers(nil, alice))
	s.Require().NoError(svc.PrincipalCanAccess(ctxFor("alice"), "photos"))
}

func (s *ACLSuite) TestDefaultAllow_AliceCanAccessTeam() {
	alice := config.BasicAuthConfigUser{Username: "alice", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder", Volumes: []string{"photos", "team"}}
	svc := buildService(s.T(), authWithUsers(nil, alice))
	s.Require().NoError(svc.PrincipalCanAccess(ctxFor("alice"), "team"))
}

func (s *ACLSuite) TestDefaultAllow_BobCanAccessTeam() {
	bob := config.BasicAuthConfigUser{Username: "bob", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder", Volumes: []string{"team"}}
	svc := buildService(s.T(), authWithUsers(nil, bob))
	s.Require().NoError(svc.PrincipalCanAccess(ctxFor("bob"), "team"))
}

func (s *ACLSuite) TestDefaultAllow_BobCannotAccessPhotos() {
	bob := config.BasicAuthConfigUser{Username: "bob", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder", Volumes: []string{"team"}}
	svc := buildService(s.T(), authWithUsers(nil, bob))
	err := svc.PrincipalCanAccess(ctxFor("bob"), "photos")
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.PermissionDenied, st.Code())
}

func (s *ACLSuite) TestDefaultAllow_CarolCanAccessPhotos() {
	// carol has no volumes key → nil → default_allow=true → allowed
	carol := config.BasicAuthConfigUser{Username: "carol", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder"}
	svc := buildService(s.T(), authWithUsers(nil, carol))
	s.Require().NoError(svc.PrincipalCanAccess(ctxFor("carol"), "photos"))
}

func (s *ACLSuite) TestDefaultAllow_CarolCanAccessTeam() {
	carol := config.BasicAuthConfigUser{Username: "carol", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder"}
	svc := buildService(s.T(), authWithUsers(nil, carol))
	s.Require().NoError(svc.PrincipalCanAccess(ctxFor("carol"), "team"))
}

// ── Tests: default_allow = false ────────────────────────────────────────────

func (s *ACLSuite) TestDefaultDeny_CarolDeniedPhotos() {
	carol := config.BasicAuthConfigUser{Username: "carol", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder"}
	svc := buildService(s.T(), authWithUsers(boolPtr(false), carol))
	err := svc.PrincipalCanAccess(ctxFor("carol"), "photos")
	s.Require().Error(err)
	st, _ := status.FromError(err)
	s.Equal(codes.PermissionDenied, st.Code())
}

func (s *ACLSuite) TestDefaultDeny_CarolDeniedTeam() {
	carol := config.BasicAuthConfigUser{Username: "carol", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder"}
	svc := buildService(s.T(), authWithUsers(boolPtr(false), carol))
	err := svc.PrincipalCanAccess(ctxFor("carol"), "team")
	s.Require().Error(err)
	st, _ := status.FromError(err)
	s.Equal(codes.PermissionDenied, st.Code())
}

func (s *ACLSuite) TestDefaultDeny_AliceStillCanAccessPhotos() {
	alice := config.BasicAuthConfigUser{Username: "alice", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder", Volumes: []string{"photos", "team"}}
	svc := buildService(s.T(), authWithUsers(boolPtr(false), alice))
	s.Require().NoError(svc.PrincipalCanAccess(ctxFor("alice"), "photos"))
}

func (s *ACLSuite) TestDefaultDeny_BobStillOnlyAccessTeam() {
	bob := config.BasicAuthConfigUser{Username: "bob", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder", Volumes: []string{"team"}}
	svc := buildService(s.T(), authWithUsers(boolPtr(false), bob))
	s.Require().NoError(svc.PrincipalCanAccess(ctxFor("bob"), "team"))
	err := svc.PrincipalCanAccess(ctxFor("bob"), "photos")
	s.Require().Error(err)
	st, _ := status.FromError(err)
	s.Equal(codes.PermissionDenied, st.Code())
}

// ── Test: no principal in ctx ────────────────────────────────────────────────

func (s *ACLSuite) TestNoPrincipal_Denied() {
	alice := config.BasicAuthConfigUser{Username: "alice", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder", Volumes: []string{"photos"}}
	svc := buildService(s.T(), authWithUsers(nil, alice))
	err := svc.PrincipalCanAccess(context.Background(), "photos")
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.PermissionDenied, st.Code())
}

// ── Test: List filters by ACL ────────────────────────────────────────────────

func (s *ACLSuite) TestList_BobSeesOnlyTeam() {
	bob := config.BasicAuthConfigUser{Username: "bob", PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder", Volumes: []string{"team"}}
	svc := buildService(s.T(), authWithUsers(nil, bob))
	volumes, err := svc.List(ctxFor("bob"))
	s.Require().NoError(err)
	s.Require().Len(volumes, 1)
	s.Equal("team", volumes[0].Name)
}

// ── Test: no auth config → no restrictions ───────────────────────────────────

func (s *ACLSuite) TestNoAuth_AllAllowed() {
	svc := buildService(s.T(), nil)
	s.Require().NoError(svc.PrincipalCanAccess(context.Background(), "photos"))
	s.Require().NoError(svc.PrincipalCanAccess(context.Background(), "team"))
}
