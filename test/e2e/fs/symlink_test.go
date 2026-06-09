package fs

import (
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// SymlinkFSTestSuite covers the user-visible promise of the Readlink/Symlink
// pair: a symlink under the mount can be both read (Readlink) and created
// (Symlink) by the local process, and following an in-tree symlink resolves
// to the target's content. This is the gap surfaced during Phase 2 e2e —
// before this change, every symlink under the mount returned EOPNOTSUPP.
type SymlinkFSTestSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
}

func (s *SymlinkFSTestSuite) SetupSuite() {
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	s.Require().NoError(testAppCtx.Start())
	s.testAppCtx = testAppCtx
	// Safety net: a failed Require below skips TearDownSuite; Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = testAppCtx.Close() })
	s.volume = s.testAppCtx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.Require().NoError(s.testAppCtx.MountVolumeErr(s.volume))
}

func (s *SymlinkFSTestSuite) TearDownSuite() {
	s.Require().NoError(s.testAppCtx.Close())
}

// TestPlantedSymlinkFollowsToTarget plants a symlink server-side and
// confirms the FUSE client reads it AND follows it to the target's bytes.
// This is the positive control Phase 2 had to drop because the client
// returned EOPNOTSUPP — Readlink wasn't implemented end-to-end.
func (s *SymlinkFSTestSuite) TestPlantedSymlinkFollowsToTarget() {
	target := filepath.Join(s.volume.GetSrcPath(), "real.txt")
	s.Require().NoError(os.WriteFile(target, []byte("legit"), 0o644))
	defer os.Remove(target) //nolint:errcheck

	link := filepath.Join(s.volume.GetSrcPath(), "follow-me")
	s.Require().NoError(os.Symlink("real.txt", link))
	defer os.Remove(link) //nolint:errcheck

	// Readlink path: os.Readlink returns the literal target string.
	got, err := os.Readlink(filepath.Join(s.volume.GetMountPath(), "follow-me"))
	s.Require().NoError(err)
	s.Equal("real.txt", got, "Readlink must return the link's literal target")

	// Traversal path: os.ReadFile chases the symlink and returns the target's bytes.
	content, err := os.ReadFile(filepath.Join(s.volume.GetMountPath(), "follow-me"))
	s.Require().NoError(err, "kernel must be able to follow the in-tree symlink")
	s.Equal("legit", string(content))
}

// TestSymlinkCreatedThroughMountIsReadable creates a symlink via os.Symlink
// THROUGH the mount, then reads it back. End-to-end exercise of the new
// Symlink RPC + Readlink RPC together.
func (s *SymlinkFSTestSuite) TestSymlinkCreatedThroughMountIsReadable() {
	target := filepath.Join(s.volume.GetSrcPath(), "src.txt")
	s.Require().NoError(os.WriteFile(target, []byte("through-mount"), 0o644))
	defer os.Remove(target) //nolint:errcheck

	linkOnMount := filepath.Join(s.volume.GetMountPath(), "mount-created-link")
	s.Require().NoError(os.Symlink("src.txt", linkOnMount),
		"creating a symlink through the mount must succeed (Symlink RPC)")
	defer os.Remove(filepath.Join(s.volume.GetSrcPath(), "mount-created-link")) //nolint:errcheck

	got, err := os.Readlink(linkOnMount)
	s.Require().NoError(err)
	s.Equal("src.txt", got)

	content, err := os.ReadFile(linkOnMount)
	s.Require().NoError(err)
	s.Equal("through-mount", string(content))
}

func TestSymlinkFSTestSuite(t *testing.T) {
	suite.Run(t, new(SymlinkFSTestSuite))
}
