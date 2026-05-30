package api

// acl_test.go exercises per-user volume ACLs end-to-end at the gRPC API level
// (no FUSE mount required). Two volumes — "photos" and "team" — are registered
// on a server with auth.type:basic and default_allow:false. Alice is granted
// both volumes; Bob is granted only "team".
//
// Assertions:
//   - Bob → GetAttr on "team"   → gRPC error nil, reply.Status == 0 (OK).
//   - Bob → GetAttr on "photos" → gRPC error with codes.PermissionDenied.
//   - Bob → List               → returns only ["team"].
//   - Alice → GetAttr on "photos" → gRPC error nil, reply.Status == 0 (OK).

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// VolumeACLSuite verifies per-principal volume ACL enforcement at the gRPC level.
type VolumeACLSuite struct {
	suite.Suite

	app        *utils.AppTestingContext
	photosName string // server-side volume name for "photos"
	teamName   string // server-side volume name for "team"
}

func (s *VolumeACLSuite) SetupSuite() {
	photosDir := s.T().TempDir()
	teamDir := s.T().TempDir()
	// Use fixed volume names so that the ACL grants can reference them.
	s.photosName = "photos"
	s.teamName = "team"

	app, err := utils.NewAppTestingContext(
		utils.WithACLUsers(
			utils.ACLUser{Username: "alice", Password: "alicepass", Volumes: []string{"photos", "team"}},
			utils.ACLUser{Username: "bob", Password: "bobpass", Volumes: []string{"team"}},
		),
		utils.WithExistingVolume("photos", photosDir),
		utils.WithExistingVolume("team", teamDir),
	)
	s.Require().NoError(err)
	s.Require().NoError(app.Start())
	s.app = app
}

func (s *VolumeACLSuite) TearDownSuite() {
	if s.app != nil {
		_ = s.app.Close()
	}
}

// caller returns a minimal Caller so createContext in the controller doesn't
// encounter a nil Caller — passthrough mapping reads caller.Owner.Uid/Gid.
func caller() *proto.Caller {
	return &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}}
}

// --- Bob tests ---

// TestBobCanAccessTeam verifies that bob (granted "team") gets a successful
// GetAttr on the team volume root.
func (s *VolumeACLSuite) TestBobCanAccessTeam() {
	bobClient, err := s.app.NewClientAs("bob", "bobpass")
	s.Require().NoError(err)
	defer func() { _ = bobClient.Close() }()

	ctx := context.Background()
	reply, err := bobClient.Fs().GetAttr(ctx, &proto.GetAttrRequest{
		Volume: s.teamName,
		Path:   "",
		Caller: caller(),
	})
	s.Require().NoError(err, "bob must be allowed on team volume")
	s.EqualValues(0, reply.Status, "GetAttr root of team must return status 0 (OK)")
}

// TestBobDeniedPhotos verifies that bob (not granted "photos") receives a
// PermissionDenied gRPC error (not a reply.Status) when calling GetAttr.
func (s *VolumeACLSuite) TestBobDeniedPhotos() {
	bobClient, err := s.app.NewClientAs("bob", "bobpass")
	s.Require().NoError(err)
	defer func() { _ = bobClient.Close() }()

	ctx := context.Background()
	_, err = bobClient.Fs().GetAttr(ctx, &proto.GetAttrRequest{
		Volume: s.photosName,
		Path:   "",
		Caller: caller(),
	})
	s.Require().Error(err, "bob must be denied on photos volume")
	st, ok := status.FromError(err)
	s.Require().True(ok, "error must be a gRPC status error")
	s.Equal(codes.PermissionDenied, st.Code(),
		"denial must be PermissionDenied, got %s: %s", st.Code(), st.Message())
}

// TestBobListSeesOnlyTeam verifies that volume list returns only the volumes
// bob is granted (team), not photos.
func (s *VolumeACLSuite) TestBobListSeesOnlyTeam() {
	bobClient, err := s.app.NewClientAs("bob", "bobpass")
	s.Require().NoError(err)
	defer func() { _ = bobClient.Close() }()

	ctx := context.Background()
	reply, err := bobClient.Volume().List(ctx, &proto.VolumeListRequest{})
	s.Require().NoError(err, "List must not error")
	names := make([]string, 0, len(reply.Volumes))
	for _, v := range reply.Volumes {
		names = append(names, v.Name)
	}
	s.ElementsMatch([]string{s.teamName}, names,
		"bob must see only team in the volume list, got %v", names)
}

// --- Alice tests ---

// TestAliceCanAccessPhotos verifies that alice (granted both volumes) gets a
// successful GetAttr on the photos volume root.
func (s *VolumeACLSuite) TestAliceCanAccessPhotos() {
	// The default client in the fixture is alice (first user in WithACLUsers).
	ctx := context.Background()
	reply, err := s.app.GetClient().Fs().GetAttr(ctx, &proto.GetAttrRequest{
		Volume: s.photosName,
		Path:   "",
		Caller: caller(),
	})
	s.Require().NoError(err, "alice must be allowed on photos volume")
	s.EqualValues(0, reply.Status, "GetAttr root of photos must return status 0 (OK)")
}

func TestVolumeACLSuite(t *testing.T) {
	suite.Run(t, new(VolumeACLSuite))
}
