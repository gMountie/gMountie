package fs

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	clientConfig "go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// WritebackErrorSuite verifies that a write failure on the server's backing
// store (here: ENOSPC from a tiny tmpfs) is delivered to the application under
// the writeback page cache rather than being silently lost. The error may
// surface at write() (the kernel flushes dirty pages early) or at Close() (the
// close-tail flush). Requires root (tmpfs mount, via passwordless sudo) and a
// real FUSE mount — runs on the VM.
type WritebackErrorSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
	mnt        string
	tmpfsDir   string
}

func (s *WritebackErrorSuite) SetupSuite() {
	dir, err := os.MkdirTemp("", "gmountie-tmpfs-*")
	if err != nil {
		s.T().Fatal(err)
	}
	s.tmpfsDir = dir
	// mode=1777 so the (non-root) server process can create files in the tmpfs.
	out, err := exec.Command("sudo", "mount", "-t", "tmpfs", "-o", "size=2m,mode=1777", "tmpfs", dir).CombinedOutput()
	if err != nil {
		_ = os.RemoveAll(dir)
		s.T().Skipf("cannot mount tmpfs (need passwordless sudo): %v: %s", err, out)
	}

	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume("wberr", dir),
		utils.WithFUSEConfig(clientConfig.FUSEConfig{
			MaxWriteBytes:  clientConfig.DefaultFUSEMaxWriteBytes,
			MaxBackground:  64,
			WritebackCache: true,
		}),
	)
	if err != nil {
		_ = exec.Command("sudo", "umount", dir).Run()
		_ = os.RemoveAll(dir)
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

func (s *WritebackErrorSuite) TearDownSuite() {
	if s.testAppCtx != nil {
		if err := s.testAppCtx.Close(); err != nil {
			s.T().Logf("app context close error: %v", err)
		}
	}
	if s.tmpfsDir != "" {
		_ = exec.Command("sudo", "umount", s.tmpfsDir).Run()
		_ = os.RemoveAll(s.tmpfsDir)
	}
}

// TestWritePastCapacityDeliversENOSPC writes far past the 2 MiB tmpfs capacity
// through a writeback-cached mount and asserts the application receives ENOSPC
// — at write() or at Close() — rather than the failure being silently dropped.
func (s *WritebackErrorSuite) TestWritePastCapacityDeliversENOSPC() {
	f, err := os.OpenFile(filepath.Join(s.mnt, "big.bin"), os.O_CREATE|os.O_WRONLY, 0o644)
	s.Require().NoError(err)

	buf := bytes.Repeat([]byte("z"), 1<<20) // 1 MiB
	var writeErr error
	for i := 0; i < 8 && writeErr == nil; i++ {
		_, writeErr = f.Write(buf)
	}
	closeErr := f.Close()

	s.T().Logf("write err=%v, close err=%v", writeErr, closeErr)
	s.Require().True(
		errors.Is(writeErr, syscall.ENOSPC) || errors.Is(closeErr, syscall.ENOSPC),
		"expected ENOSPC delivered at write or close; got write=%v close=%v", writeErr, closeErr,
	)
}

func TestWritebackErrorSuite(t *testing.T) {
	suite.Run(t, new(WritebackErrorSuite))
}
