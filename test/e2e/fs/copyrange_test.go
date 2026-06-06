package fs

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type CopyRangeE2ESuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
}

func TestCopyRangeE2ESuite(t *testing.T) { suite.Run(t, new(CopyRangeE2ESuite)) }

func (s *CopyRangeE2ESuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(true),
	)
	s.Require().NoError(err)
	utils.Must0(s.T(), ctx.Start())
	s.testAppCtx = ctx
	s.volume = ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	ctx.MountVolume(s.volume)
}

func (s *CopyRangeE2ESuite) TearDownSuite() {
	s.Require().NoError(s.testAppCtx.Close())
}

// serverBytesOut reads the server's streaming-Read byte counter for the
// test volume. Only the streaming Read handler increments it, so a delta
// of ~0 across a copy proves no file data crossed the wire — the spec's
// acceptance criterion, measured deterministically (RPC byte counts, not
// timing).
func (s *CopyRangeE2ESuite) serverBytesOut() float64 {
	m := s.testAppCtx.GetServerApp().Metrics
	return testutil.ToFloat64(m.Bytes.WithLabelValues(s.volume.Name, "out"))
}

// TestCopyFileRangeSyscall drives copy_file_range(2) directly against the
// mount (no libc/GIO fallback in the way), verifies content fidelity, AND
// asserts the copy was server-side. The latter is the assertion that
// matters: the syscall succeeds even on broken wiring (the kernel falls
// back to a generic in-kernel copy), so success alone proves nothing.
func (s *CopyRangeE2ESuite) TestCopyFileRangeSyscall() {
	mount := s.volume.GetMountPath()
	content := make([]byte, 4<<20)
	_, err := rand.Read(content)
	s.Require().NoError(err)
	srcPath := filepath.Join(mount, "cfr-src.bin")
	dstPath := filepath.Join(mount, "cfr-dst.bin")
	s.Require().NoError(os.WriteFile(srcPath, content, 0o644))

	src, err := os.Open(srcPath)
	s.Require().NoError(err)
	defer src.Close()
	dst, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE, 0o644)
	s.Require().NoError(err)
	defer dst.Close()

	outBefore := s.serverBytesOut()

	var total int
	for total < len(content) {
		n, err := unix.CopyFileRange(int(src.Fd()), nil, int(dst.Fd()), nil, len(content)-total, 0)
		s.Require().NoError(err, "copy_file_range through the mount")
		s.Require().Positive(n)
		total += n
	}

	// Acceptance: server-side copy streams (almost) nothing to the client.
	// A fallback that round-trips the data would show a ~4 MiB delta;
	// allow 64 KiB slack for incidental kernel reads around the copy.
	outAfter := s.serverBytesOut()
	s.Require().Less(outAfter-outBefore, float64(64<<10),
		"copy must not stream file data through the client (delta=%v)", outAfter-outBefore)

	got, err := os.ReadFile(dstPath) // after the metrics check — this read legitimately streams
	s.Require().NoError(err)
	s.Require().True(bytes.Equal(content, got), "destination content must match source")
}

// TestSeekDataHole exercises FUSE_LSEEK through the mount using
// FS-agnostic invariants (hole reporting granularity varies by backing
// filesystem, so don't assert exact hole offsets for punched ranges).
// This test doubles as the production-chain guard for RawFdFile: if any
// FS wrapper stopped passing fd-backed files through, server Lseek would
// return ENOTSUP and this fails loudly.
func (s *CopyRangeE2ESuite) TestSeekDataHole() {
	p := filepath.Join(s.volume.GetMountPath(), "sparse.bin")
	s.Require().NoError(os.WriteFile(p, []byte("0123456789"), 0o644))
	f, err := os.Open(p)
	s.Require().NoError(err)
	defer f.Close()

	off, err := unix.Seek(int(f.Fd()), 0, unix.SEEK_DATA)
	s.Require().NoError(err)
	s.Equal(int64(0), off)

	off, err = unix.Seek(int(f.Fd()), 0, unix.SEEK_HOLE)
	s.Require().NoError(err)
	s.GreaterOrEqual(off, int64(10)) // implicit EOF hole

	_, err = unix.Seek(int(f.Fd()), 100, unix.SEEK_DATA)
	s.Require().ErrorIs(err, unix.ENXIO)
}

// TestXattrRoundTrip exercises set/get/list/remove through the mount and
// the server-side namespace policy.
func (s *CopyRangeE2ESuite) TestXattrRoundTrip() {
	p := filepath.Join(s.volume.GetMountPath(), "xattr.txt")
	s.Require().NoError(os.WriteFile(p, []byte("x"), 0o644))

	if err := unix.Setxattr(p, "user.e2e", []byte("v1"), 0); err == unix.ENOTSUP {
		s.T().Skip("backing filesystem has no xattr support")
	} else {
		s.Require().NoError(err)
	}

	buf := make([]byte, 64)
	n, err := unix.Getxattr(p, "user.e2e", buf)
	s.Require().NoError(err)
	s.Equal([]byte("v1"), buf[:n])

	// XATTR_CREATE on an existing attr must round-trip EEXIST.
	err = unix.Setxattr(p, "user.e2e", []byte("v2"), unix.XATTR_CREATE)
	s.Require().ErrorIs(err, unix.EEXIST)

	n, err = unix.Listxattr(p, buf)
	s.Require().NoError(err)
	s.Contains(string(buf[:n]), "user.e2e")

	// Policy: trusted.* writes must be rejected by the SERVER (EPERM),
	// regardless of local privileges.
	err = unix.Setxattr(p, "trusted.e2e", []byte("v"), 0)
	s.Require().ErrorIs(err, unix.EPERM)

	s.Require().NoError(unix.Removexattr(p, "user.e2e"))
	_, err = unix.Getxattr(p, "user.e2e", buf)
	s.Require().ErrorIs(err, unix.ENODATA)
}
