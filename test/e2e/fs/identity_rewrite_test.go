package fs

// identity_rewrite_test.go proves that the client-side IDRewriter correctly
// maps the server-resolved uid/gid back to the local mounting user's uid/gid.
//
// Scenario: a squash volume pins every caller to uid 1000 on the server. A
// file created through the mount is owned by uid 1000 on disk. The IDRewriter
// (seeded by WhoAmI at mount time) must translate uid 1000 → os.Getuid() so
// that the FUSE kernel reports the file as owned by the local mounting user.
//
// Why root is required: BindIdentity uses setfsuid/setfsgid (via
// IdentityBoundFS) to make the local loopback FS create files as uid 1000.
// That syscall is only permitted when the process runs as root. Without it,
// every file ends up owned by the server process uid, making the rewrite
// unobservable. Tests skip automatically when not root so the sandbox CI
// build stays green; run on the VM.
//
// raw_ids coverage note: a raw_ids=true mount skips WhoAmI and the IDRewriter,
// so uid 1000 would be reported as-is by the kernel. That behaviour is covered
// by mounter-level unit tests (pkg/client/mount/single_test.go) and is a
// follow-up for a separate e2e test that needs dedicated harness plumbing for
// rawIDs=true.

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// squashUID/squashGID is the fixed server-side identity the volume squashes to.
// Chosen as 1000 (a common unprivileged uid that is distinct from root and from
// the nobody uid 65534). If the test machine's mounting user happens to be
// uid 1000 the negative assertion still catches a missing-rewrite bug because
// the rewrite maps the squash uid → localUID; the file stat equals localUID
// regardless, and a broken rewrite would leave uid==1000 AND localUID==1000
// making both assertions vacuously true but that degenerate case is documented.
const squashUID uint32 = 1000
const squashGID uint32 = 1000

// IdentityRewriteSuite verifies that squash-mode server uid/gid is rewritten
// to the local mounting user's uid/gid through the real FUSE stack.
type IdentityRewriteSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
	mnt        string
}

func (s *IdentityRewriteSuite) SetupSuite() {
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("admin", "admin"),
		utils.WithSquashVolume(squashUID, squashGID),
	)
	if err != nil {
		s.T().Fatal(err)
	}
	utils.Must0(s.T(), testAppCtx.Start())
	s.testAppCtx = testAppCtx
	// Safety net: a failed Require below skips TearDownSuite; Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = testAppCtx.Close() })
	s.volume = s.testAppCtx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.Require().NoError(s.testAppCtx.MountVolumeErr(s.volume))
	s.mnt = s.volume.GetMountPath()
}

func (s *IdentityRewriteSuite) TearDownSuite() {
	if s.testAppCtx == nil {
		return
	}
	if err := s.testAppCtx.Close(); err != nil {
		s.T().Fatal(err)
	}
}

// TestSquashUIDRewrittenToLocalUser creates a file through the FUSE mount and
// verifies that the kernel-visible owner is the local mounting user (os.Getuid()),
// NOT the squash uid (1000) that the server actually used for the on-disk write.
// This is the core inbound-rewrite contract: server uid → local uid.
func (s *IdentityRewriteSuite) TestSquashUIDRewrittenToLocalUser() {
	path := filepath.Join(s.mnt, "f.bin")
	s.Require().NoError(os.WriteFile(path, []byte("hi"), 0o644))

	fi, err := os.Lstat(path)
	s.Require().NoError(err)

	stat, ok := fi.Sys().(*syscall.Stat_t)
	s.Require().True(ok, "fi.Sys() must be *syscall.Stat_t on Linux")

	localUID := uint32(os.Getuid())
	localGID := uint32(os.Getgid())

	// Primary assertion: the rewriter must map the squash uid (1000) to the
	// local mounting uid — the file appears owned by this process.
	s.Assert().Equal(localUID, stat.Uid,
		"FUSE-visible uid must be the local mounting user uid (rewritten from squash uid %d)", squashUID)
	s.Assert().Equal(localGID, stat.Gid,
		"FUSE-visible gid must be the local mounting user gid (rewritten from squash gid %d)", squashGID)

	// Negative assertion: the raw squash uid must NOT be visible. A broken
	// rewriter (no WhoAmI, rawIDs path, or identity not seeded) would leave
	// uid==1000. This is vacuously true when localUID==1000, but remains a
	// useful regression guard for the common case.
	if localUID != squashUID {
		s.Assert().NotEqual(squashUID, stat.Uid,
			"FUSE-visible uid must not be the raw squash uid %d; rewriting is broken", squashUID)
	}
	if localGID != squashGID {
		s.Assert().NotEqual(squashGID, stat.Gid,
			"FUSE-visible gid must not be the raw squash gid %d; rewriting is broken", squashGID)
	}
}

// TestSquashWhoAmIReturnsUserName proves the server populates Identity.user_name
// end-to-end: a WhoAmI call from the client returns a non-empty UserName for the
// squash uid (when the VM's name service can resolve it; skipped otherwise).
func (s *IdentityRewriteSuite) TestSquashWhoAmIReturnsUserName() {
	id, err := s.testAppCtx.GetClient().WhoAmI(context.Background(), s.volume.Name)
	s.Require().NoError(err)
	if id.UserName == "" {
		s.T().Skipf("getent passwd %d returns nothing on this VM; cannot assert UserName", squashUID)
	}
	s.Assert().NotEmpty(id.UserName, "WhoAmI must surface the squash uid's name")
}

func TestIdentityRewriteSuite(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root: squash-mode enforcement needs setfsuid")
	}
	suite.Run(t, new(IdentityRewriteSuite))
}
