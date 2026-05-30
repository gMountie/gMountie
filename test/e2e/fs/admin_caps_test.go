package fs

// admin_caps_test.go exercises the two admin capabilities end-to-end through
// the full FUSE + gRPC + static-mapping stack with real basic-auth principals.
//
// Threat model (spec §10 adapted for caps):
//
//   reader (uid 65501, cap dac_read_search):
//     Can read a 0o600 file owned by a third uid (65500, "victim") — DAC_READ_SEARCH
//     lets the kernel pass the open even though the file is not owned by the caller
//     and has no group/other read bits. Cannot write that file — DAC_OVERRIDE is
//     absent from the effective capability set.
//
//   admin (uid 65502, cap dac_override):
//     Can both read AND write the same 0o600 victim-owned file. New entries
//     created through the mount land on-disk with uid/gid 65502 because the
//     bound-FS performs a post-create fchown (T5 implementation).
//
// The protected file is owned by uid 65500 (not root, not reader, not admin)
// so that the DAC "owner == fsuid" shortcut does not bypass the cap check.
// Both cap paths keep fsuid=0 (per applyDacReadSearch / applyDacOverride), so
// using a root-owned file would let the write through via owner-match; victim
// ownership forces the kernel to consult the effective capability set.
//
// Two independent AppTestingContext instances (one per principal) share the
// same on-disk src directory. Each context has its own server with a single
// basic-auth user; both servers are configured with a static-mode volume
// whose mapping table contains only the one principal for that server.
//
// Ownership assertions inspect the real on-disk path (sharedSrc/...) rather
// than the FUSE-visible path so they are immune to the client-side IDRewriter.
//
// Root + FUSE required: run on the kubevirt VM.

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	srvconfig "go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

const (
	readerUID uint32 = 65501
	readerGID uint32 = 65501
	adminUID  uint32 = 65502
	adminGID  uint32 = 65502

	// victimUID/GID is the owner of the protected file — distinct from both
	// reader and admin so ownership-based DAC grants do not apply. Must also
	// be non-root so that fsuid=0 (retained by both cap paths) does not
	// trigger the "owner == process fsuid" DAC shortcut.
	victimUID uint32 = 65500
	victimGID uint32 = 65500
)

// AdminCapsSuite verifies the dac_read_search and dac_override cap paths
// through the real FUSE mount + gRPC + static identity resolver.
type AdminCapsSuite struct {
	suite.Suite
	appReader     *utils.AppTestingContext
	appAdmin      *utils.AppTestingContext
	volReader     *utils.TestVolume
	volAdmin      *utils.TestVolume
	sharedSrc     string // on-disk directory both volumes point at
	protectedFile string // basename of the 0o600 victim-owned file
}

func (s *AdminCapsSuite) SetupSuite() {
	// 1. Create the shared src directory.
	// Use /tmp directly (mode 1777) rather than os.MkdirTemp("", ...) which
	// creates a 0o700 root-owned dir — that blocks traversal once the kernel
	// checks directory permission against the mapped uid.
	parent, err := os.MkdirTemp("/tmp", "gmountie-admincaps-*")
	s.Require().NoError(err)
	s.Require().NoError(os.Chmod(parent, 0o755))
	s.sharedSrc = filepath.Join(parent, "src")
	s.Require().NoError(os.Mkdir(s.sharedSrc, 0o755))
	s.T().Cleanup(func() { _ = os.RemoveAll(parent) })

	// 2. Record the protected-file basename; content is (re)planted by SetupTest
	//    so each test sees a fresh "classified" payload regardless of run order.
	s.protectedFile = "secret.txt"

	// 3. Build the reader context: uid 65501, cap dac_read_search.
	s.appReader, s.volReader = s.buildContext(
		"reader", "reader-pass",
		readerUID, readerGID, []string{"dac_read_search"},
	)

	// 4. Build the admin context: uid 65502, cap dac_override.
	s.appAdmin, s.volAdmin = s.buildContext(
		"admin", "admin-pass",
		adminUID, adminGID, []string{"dac_override"},
	)
}

// buildContext creates an AppTestingContext whose server has a static mapping
// with a single principal (user/pass) mapped to uid/gid with the given caps,
// and whose client authenticates as that principal. Both contexts share the
// same on-disk sharedSrc via WithStaticVolume.
func (s *AdminCapsSuite) buildContext(
	user, pass string,
	uid, gid uint32,
	caps []string,
) (*utils.AppTestingContext, *utils.TestVolume) {
	users := map[string]srvconfig.StaticUser{
		user: {Uid: uid, Gid: gid, Caps: caps},
	}

	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth(user, pass),
		utils.WithStaticVolume(s.sharedSrc, users, nil),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())

	vols := ctx.GetVolumes()
	s.Require().Len(vols, 1, "expected exactly one volume in context for %q", user)
	vol := vols[0]
	ctx.MountVolume(vol)
	return ctx, vol
}

// SetupTest re-plants secret.txt before every test method so tests are
// independent of execution order. The file is owned by victimUID:victimGID
// (65500:65500) with mode 0o600 — distinct from both reader (65501) and admin
// (65502) so the DAC owner-shortcut does not apply. Also distinct from root so
// that fsuid=0 (retained by both cap paths) does not match the owner uid.
func (s *AdminCapsSuite) SetupTest() {
	secretPath := filepath.Join(s.sharedSrc, s.protectedFile)
	s.Require().NoError(os.WriteFile(secretPath, []byte("classified"), 0o600))
	s.Require().NoError(os.Chown(secretPath, int(victimUID), int(victimGID)))
	s.Require().NoError(os.Chmod(secretPath, 0o600))
}

func (s *AdminCapsSuite) TearDownSuite() {
	if s.appReader != nil {
		s.Require().NoError(s.appReader.Close())
	}
	if s.appAdmin != nil {
		s.Require().NoError(s.appAdmin.Close())
	}
}

// TestReaderCanReadDeniedWrite verifies that dac_read_search lets the reader
// open a victim-owned (uid 65500) 0o600 file for reading but denies writing it.
func (s *AdminCapsSuite) TestReaderCanReadDeniedWrite() {
	p := filepath.Join(s.volReader.GetMountPath(), s.protectedFile)

	// Read must succeed.
	content, err := os.ReadFile(p)
	s.Require().NoError(err, "reader with dac_read_search must read victim-owned 0o600 file")
	s.Equal("classified", string(content))

	// Write must fail — dac_override is absent from the reader's effective caps.
	err = os.WriteFile(p, []byte("modified"), 0o600)
	s.Require().Error(err, "reader without dac_override must not write foreign 0o600 file")
	s.True(os.IsPermission(err), "expected permission error writing foreign file, got: %v", err)
}

// TestAdminWritesAndCreates verifies that dac_override lets the admin write a
// root-owned 0o600 file and create new entries that land owned by the admin's
// uid (post-create fchown), not root.
func (s *AdminCapsSuite) TestAdminWritesAndCreates() {
	p := filepath.Join(s.volAdmin.GetMountPath(), s.protectedFile)

	// Write the existing victim-owned file.
	s.Require().NoError(os.WriteFile(p, []byte("audited-change"), 0o600),
		"admin with dac_override must write victim-owned 0o600 file")

	// Create a new file through the mount; it should land owned by adminUID.
	newFile := filepath.Join(s.volAdmin.GetMountPath(), "admin-created.txt")
	s.Require().NoError(os.WriteFile(newFile, []byte("hi"), 0o644))
	info, err := os.Stat(filepath.Join(s.sharedSrc, "admin-created.txt"))
	s.Require().NoError(err)
	st := info.Sys().(*syscall.Stat_t)
	s.Equal(adminUID, st.Uid,
		"dac_override Create must fchown new file to principal uid %d (got %d)", adminUID, st.Uid)

	// Mkdir through the mount; it should also land owned by adminUID.
	newDir := filepath.Join(s.volAdmin.GetMountPath(), "admin-created-dir")
	s.Require().NoError(os.Mkdir(newDir, 0o755))
	dinfo, err := os.Stat(filepath.Join(s.sharedSrc, "admin-created-dir"))
	s.Require().NoError(err)
	dst := dinfo.Sys().(*syscall.Stat_t)
	s.Equal(adminUID, dst.Uid,
		"dac_override Mkdir must fchown new dir to principal uid %d (got %d)", adminUID, dst.Uid)
}

func TestAdminCapsSuite(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("admin caps e2e requires root (per-thread capset + setfsuid)")
	}
	suite.Run(t, new(AdminCapsSuite))
}
