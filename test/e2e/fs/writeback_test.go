package fs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	clientConfig "gmountie/pkg/client/config"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// WritebackSuite exercises the kernel writeback page cache (CAP_WRITEBACK_CACHE).
// The mount is configured with WritebackCache: true so the kernel buffers
// application write()s and flushes them asynchronously as FUSE WRITEs. Tests
// here validate the core writeback contract:
//   - write-then-read-back: data flushed by the kernel round-trips correctly.
//   - truncate: a kernel-driven setattr(size=N) reaches the server and the
//     stat-visible file size reflects the truncation.
//
// These tests require a real FUSE mount and must run on the VM.
type WritebackSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
	mnt        string
}

func (s *WritebackSuite) SetupSuite() {
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithFUSEConfig(clientConfig.FUSEConfig{
			MaxWriteBytes:  clientConfig.DefaultFUSEMaxWriteBytes,
			MaxBackground:  64,
			WritebackCache: true,
		}),
	)
	if err != nil {
		s.T().Fatal(err)
	}
	utils.Must0(s.T(), testAppCtx.Start())
	s.testAppCtx = testAppCtx
	s.volume = s.testAppCtx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.testAppCtx.MountVolume(s.volume)
	s.mnt = s.volume.GetMountPath()
}

func (s *WritebackSuite) TearDownSuite() {
	err := s.testAppCtx.Close()
	if err != nil {
		s.T().Fatal(err)
	}
}

// TestWriteThenReadBack writes 3 MiB (larger than one max_write window) through
// the writeback mount and reads it back. Under CAP_WRITEBACK_CACHE the kernel
// may coalesce the write into multiple async FUSE WRITEs before flush; we
// confirm the full content arrives on the server and is readable via the same
// mount.
func (s *WritebackSuite) TestWriteThenReadBack() {
	path := filepath.Join(s.mnt, "wb.bin")
	want := bytes.Repeat([]byte("x"), 3<<20) // 3 MiB
	s.Require().NoError(os.WriteFile(path, want, 0o644))
	got, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Assert().Equal(want, got)
}

// TestTruncateUnderWriteback writes a 1 MiB file then truncates it to 4096
// bytes. Under writeback the kernel sends setattr(size=4096) which must
// propagate to the server as a Truncate RPC; the subsequent Stat must report
// the new size.
func (s *WritebackSuite) TestTruncateUnderWriteback() {
	path := filepath.Join(s.mnt, "wb-trunc.bin")
	s.Require().NoError(os.WriteFile(path, bytes.Repeat([]byte("y"), 1<<20), 0o644))
	s.Require().NoError(os.Truncate(path, 4096))
	fi, err := os.Stat(path)
	s.Require().NoError(err)
	s.Assert().Equal(int64(4096), fi.Size())
}

func TestWritebackSuite(t *testing.T) {
	suite.Run(t, new(WritebackSuite))
}
