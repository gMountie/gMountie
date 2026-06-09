package fs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	clientConfig "go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// UtimensSuite verifies that utimensat/touch through a gMountie mount persists
// atime/mtime to the server's backing file. It stats the backing source file
// directly (bypassing the mount) so a client-side cache hit cannot mask a
// missing server-side write. Requires a real FUSE mount — runs on the VM.
type UtimensSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
	mnt        string
}

func (s *UtimensSuite) SetupSuite() {
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithFUSEConfig(clientConfig.FUSEConfig{
			MaxWriteBytes:  clientConfig.DefaultFUSEMaxWriteBytes,
			MaxBackground:  64,
			WritebackCache: false,
		}),
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

func (s *UtimensSuite) TearDownSuite() {
	if err := s.testAppCtx.Close(); err != nil {
		s.T().Fatal(err)
	}
}

// TestSetMtimePersistsToBackingFile sets a fixed mtime through the mount, then
// stats the backing source file directly (bypassing gMountie) to prove the
// timestamp reached the server rather than only the client cache.
func (s *UtimensSuite) TestSetMtimePersistsToBackingFile() {
	name := "ut.bin"
	mountPath := filepath.Join(s.mnt, name)
	s.Require().NoError(os.WriteFile(mountPath, []byte("hello"), 0o644))

	want := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	s.Require().NoError(os.Chtimes(mountPath, want, want))

	// Stat the backing source file directly — proves the change reached the
	// server, not just the client cache.
	srcPath := filepath.Join(s.volume.GetSrcPath(), name)
	fi, err := os.Stat(srcPath)
	s.Require().NoError(err)
	s.Assert().Equal(want.Unix(), fi.ModTime().Unix())
}

func TestUtimensSuite(t *testing.T) {
	suite.Run(t, new(UtimensSuite))
}
