package fs

import (
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// KillPrivFSSuite verifies that with CAP_HANDLE_KILLPRIV_V2 advertised (the
// default, fuse.handle_kill_priv=true), modifying a file that has setuid/setgid
// bits still STRIPS those bits on the backing file. The cap delegates
// priv-stripping to the filesystem; this is the regression guard that the
// delegation does not silently retain the bits.
//
// Scope (passthrough mapping, non-root writer): the kernel only strips
// setuid/setgid when the writing process lacks CAP_FSETID, i.e. is non-root.
// CI and the local VM run the test process non-root, where the strip is
// observable. When run as root the test skips (root retains CAP_FSETID, so no
// strip would occur and the assertion would be meaningless). The strip is
// mapping-mode-independent (kernel issues a setattr the server applies), so a
// per-mode matrix adds no signal — see the design doc.
type KillPrivFSSuite struct {
	suite.Suite
	ctx    *utils.AppTestingContext
	volume *utils.TestVolume
}

func (s *KillPrivFSSuite) SetupSuite() {
	if os.Geteuid() == 0 {
		s.T().Skip("killpriv strip is only observable for a non-root writer " +
			"(root retains CAP_FSETID and the kernel skips file_remove_privs); " +
			"run this test as a non-root user")
	}
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false), // passthrough mapping, no pre-created files
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.ctx = ctx
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.volume = ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.Require().NoError(ctx.MountVolumeErr(s.volume))
}

func (s *KillPrivFSSuite) TearDownSuite() {
	if s.ctx != nil {
		s.Require().NoError(s.ctx.Close())
	}
}

// assertStripped writes a file through the mount with the given mode (which
// includes a setuid and/or setgid bit), modifies it, and asserts the special
// bits are cleared on the BACKING file (server-side source dir).
func (s *KillPrivFSSuite) assertStripped(name string, mode os.FileMode) {
	mp := s.volume.GetMountPath()
	src := s.volume.GetSrcPath()
	mntPath := filepath.Join(mp, name)
	backingPath := filepath.Join(src, name)

	// Create + set the special bits through the mount.
	s.Require().NoError(os.WriteFile(mntPath, []byte("aaa"), 0o644))
	s.Require().NoError(os.Chmod(mntPath, mode))

	// Confirm the bits are actually set on the backing file before the write
	// (otherwise the post-write assertion proves nothing).
	pre, err := os.Stat(backingPath)
	s.Require().NoError(err)
	special := os.ModeSetuid | os.ModeSetgid
	s.Require().NotZero(pre.Mode()&special&mode,
		"precondition: special bits must be set on the backing file before the write")

	// Modify the file (a plain write triggers the kernel's file_remove_privs).
	f, err := os.OpenFile(mntPath, os.O_WRONLY, 0)
	s.Require().NoError(err)
	_, werr := f.WriteAt([]byte("bbb"), 0)
	s.Require().NoError(werr)
	s.Require().NoError(f.Sync())
	s.Require().NoError(f.Close())

	// The setuid/setgid bits requested in `mode` must be cleared on the
	// backing file.
	post, err := os.Stat(backingPath)
	s.Require().NoError(err)
	s.Zero(post.Mode()&special,
		"setuid/setgid must be stripped on the backing file after a non-root write "+
			"(requested mode %o, backing mode after write %o)", mode, post.Mode())
}

func (s *KillPrivFSSuite) TestSetuidStrippedOnWrite() {
	s.assertStripped("suidfile", os.FileMode(0o755)|os.ModeSetuid)
}

func (s *KillPrivFSSuite) TestSetgidStrippedOnWrite() {
	s.assertStripped("sgidfile", os.FileMode(0o755)|os.ModeSetgid)
}

func (s *KillPrivFSSuite) TestSetuidSetgidStrippedOnWrite() {
	s.assertStripped("suidsgidfile", os.FileMode(0o755)|os.ModeSetuid|os.ModeSetgid)
}

func TestKillPrivFSSuite(t *testing.T) {
	suite.Run(t, new(KillPrivFSSuite))
}
