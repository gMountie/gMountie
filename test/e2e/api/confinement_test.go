package api

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"gmountie/pkg/proto"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// ConfinementWireSuite is the wire-level regression for Phase 2 (spec §3.10).
// It exercises the path the FUSE kernel CAN'T reach by issuing escape attempts
// directly over the gRPC API: a malicious client that bypasses kernel-side
// path sanitization is the realistic threat model for an internet-exposed
// server. Confinement must turn every such escape into a reply.Status of
// EACCES before the path ever touches a real syscall.
//
// Note: gMountie's FUSE controllers do not surface fuse.Status as gRPC
// errors — the FUSE status code rides as an int32 on the reply, with
// err==nil from the gRPC call itself. We assert against reply.Status.
type ConfinementWireSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
	ctx        context.Context
}

func (s *ConfinementWireSuite) SetupSuite() {
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	s.Require().NoError(testAppCtx.Start())
	s.testAppCtx = testAppCtx
	s.volume = s.testAppCtx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.ctx = context.Background()
}

func (s *ConfinementWireSuite) TearDownSuite() {
	s.Require().NoError(s.testAppCtx.Close())
}

func (s *ConfinementWireSuite) fsClient() proto.RpcFsClient {
	return s.testAppCtx.GetClient().Fs()
}

// assertGetAttrDenied issues GetAttr with `path` and asserts the reply's
// embedded FUSE status is EACCES (mapped from EXDEV/ELOOP per §3.10).
func (s *ConfinementWireSuite) assertGetAttrDenied(path string) {
	s.T().Helper()
	reply, err := s.fsClient().GetAttr(s.ctx, &proto.GetAttrRequest{
		Volume: s.volume.Name,
		Path:   path,
	})
	s.Require().NoError(err, "gRPC call itself must succeed; status rides in reply")
	s.Require().NotNil(reply)
	s.Equal(int32(syscall.EACCES), reply.Status,
		"escape %q must surface as EACCES (=%d), got %d (attr=%v)",
		path, int32(syscall.EACCES), reply.Status, reply.Attributes)
}

// TestDotDotEscapeDenied: a leading ".." in the wire path is the simplest
// escape attempt. The kernel's FUSE driver would never emit this (it
// sanitizes), but a raw gRPC caller can. Confinement rejects it.
func (s *ConfinementWireSuite) TestDotDotEscapeDenied() {
	s.assertGetAttrDenied("../../etc/hostname")
}

// TestEmbeddedDotDotEscapeDenied: ".." in the middle of an otherwise
// innocent path. Without the resolveBeneath guard, path.Clean would collapse
// this to "etc/hostname" within the root — the same silent-bypass bug the
// guard was added to catch.
func (s *ConfinementWireSuite) TestEmbeddedDotDotEscapeDenied() {
	s.assertGetAttrDenied("sub/../../../etc/hostname")
}

// TestLeadingSlashIsRelative: gMountie's wire convention treats paths with
// a leading "/" as relative to the volume root, not as host-absolute. So
// "/etc/hostname" addresses <volume_root>/etc/hostname — which doesn't
// exist, hence ENOENT, NOT EACCES. (The actual escape vectors are tested
// by the other three cases.)
func (s *ConfinementWireSuite) TestLeadingSlashIsRelative() {
	reply, err := s.fsClient().GetAttr(s.ctx, &proto.GetAttrRequest{
		Volume: s.volume.Name,
		Path:   "/etc/hostname",
	})
	s.Require().NoError(err)
	s.Require().NotNil(reply)
	s.Equal(int32(syscall.ENOENT), reply.Status,
		"leading slash addresses in-tree; should be ENOENT not EACCES, got %d", reply.Status)
}

// TestSymlinkEscapeDenied: a symlink whose target lives OUTSIDE the volume,
// planted directly in the server-side src (simulating a hostile or
// pre-existing on-disk file). The wire path names the symlink itself.
// Access(F_OK) is the simplest op that FOLLOWS the symlink — that's
// where confinement bites.
func (s *ConfinementWireSuite) TestSymlinkEscapeDenied() {
	srcLink := filepath.Join(s.volume.GetSrcPath(), "evil-link")
	s.Require().NoError(os.Symlink("/etc/hostname", srcLink))
	defer os.Remove(srcLink) //nolint:errcheck // best-effort cleanup

	reply, err := s.fsClient().Access(s.ctx, &proto.AccessRequest{
		Volume: s.volume.Name,
		Path:   "evil-link",
		Mode:   0, // F_OK — existence check; follows the symlink.
	})
	s.Require().NoError(err)
	s.Require().NotNil(reply)
	s.Equal(int32(syscall.EACCES), reply.Status,
		"symlink-to-outside-root must surface as EACCES, got %d", reply.Status)
}

// TestInTreeStillResolves is the positive control: a normal in-tree path
// must still work after confinement. Without this, an over-aggressive fix
// would pass all four escape tests by breaking ALL access.
func (s *ConfinementWireSuite) TestInTreeStillResolves() {
	legit := filepath.Join(s.volume.GetSrcPath(), "ok.txt")
	s.Require().NoError(os.WriteFile(legit, []byte("hi"), 0o644))
	defer os.Remove(legit) //nolint:errcheck

	reply, err := s.fsClient().GetAttr(s.ctx, &proto.GetAttrRequest{
		Volume: s.volume.Name,
		Path:   "ok.txt",
	})
	s.Require().NoError(err)
	s.Require().NotNil(reply)
	s.Equal(int32(0), reply.Status, "in-tree GetAttr must succeed (status OK), got %d", reply.Status)
	s.Require().NotNil(reply.Attributes)
	s.EqualValues(2, reply.Attributes.Size, "size should reflect on-disk content")
}

func TestConfinementWireSuite(t *testing.T) {
	suite.Run(t, new(ConfinementWireSuite))
}
