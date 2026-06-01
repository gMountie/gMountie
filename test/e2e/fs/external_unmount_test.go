package fs

import (
	"os/exec"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// ExternalUnmountTestSuite covers the lingering-mount bug: when a gMountie
// mount is torn down out-of-band (a direct `fusermount -u`, not gMountie's own
// Unmount), the foreground mount must notice and exit instead of blocking on
// its signal wait forever. The fix routes that wake-up through
// SingleVolumeMounter.Wait, which returns when the FUSE server exits.
type ExternalUnmountTestSuite struct {
	suite.Suite
	appCtx *utils.AppTestingContext
	volume *utils.TestVolume
}

func (s *ExternalUnmountTestSuite) SetupSuite() {
	appCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	s.Require().NoError(appCtx.Start())
	s.appCtx = appCtx
	s.volume = appCtx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.appCtx.MountVolume(s.volume)
}

func (s *ExternalUnmountTestSuite) TearDownSuite() {
	// After the external unmount the volume is already released, so Close's
	// UnmountAll is a clean no-op; it still tears down the client and server.
	s.Require().NoError(s.appCtx.Close())
}

// externalUnmount detaches the mount the way a user would from another shell.
// It prefers the unprivileged fusermount3 helper and falls back to umount
// (the e2e server runs as root), failing only if neither detaches the mount.
func (s *ExternalUnmountTestSuite) externalUnmount(path string) {
	if out, err := exec.Command("fusermount3", "-u", path).CombinedOutput(); err == nil {
		return
	} else {
		s.T().Logf("fusermount3 -u %s failed (%v: %s); falling back to umount", path, err, out)
	}
	out, err := exec.Command("umount", path).CombinedOutput()
	s.Require().NoErrorf(err, "umount %s: %s", path, out)
}

func (s *ExternalUnmountTestSuite) TestExternalUnmountWakesWait() {
	mounter := s.appCtx.GetClientApp().SingleVolumeMounter
	s.Require().True(mounter.IsVolumeMounted(s.volume.Name))

	done := make(chan struct{})
	go func() {
		mounter.Wait(s.volume.Name)
		close(done)
	}()

	// Wait must still be blocked while the filesystem is mounted.
	select {
	case <-done:
		s.FailNow("Wait returned before the filesystem was unmounted")
	case <-time.After(200 * time.Millisecond):
	}

	s.externalUnmount(s.volume.GetMountPath())

	// The external unmount must wake Wait — otherwise the mount process lingers.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.FailNow("Wait did not return after an external unmount; the mount would linger")
	}

	// Wait released the volume, so the deferred Close in the real command is a
	// clean no-op rather than a redundant unmount of an already-detached path.
	s.False(mounter.IsVolumeMounted(s.volume.Name))
}

func TestExternalUnmountTestSuite(t *testing.T) {
	suite.Run(t, new(ExternalUnmountTestSuite))
}
