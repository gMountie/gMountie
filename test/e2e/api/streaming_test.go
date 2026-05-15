package api

import (
	"crypto/rand"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"

	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// StreamingReadE2ESuite exercises the server-streaming Read path through a
// real FUSE mount. Pre-Phase-3 these would have failed with
// ResourceExhausted at the gRPC 4 MiB unary ceiling.
type StreamingReadE2ESuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
}

func (s *StreamingReadE2ESuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.testAppCtx = ctx
	s.volume = ctx.GetVolumes()[0]
	ctx.MountVolume(s.volume)
}

func (s *StreamingReadE2ESuite) TearDownSuite() {
	if s.testAppCtx != nil {
		_ = s.testAppCtx.Close()
	}
	if s.volume != nil {
		_ = s.volume.Close()
	}
}

// seedRandom writes size bytes of random content to path (server-side, not
// through the FUSE mount) and returns the SHA-256 digest of the payload.
func (s *StreamingReadE2ESuite) seedRandom(path string, size int) [sha256.Size]byte {
	s.T().Helper()
	f, err := os.Create(path)
	s.Require().NoError(err)
	defer f.Close()
	h := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(f, h), rand.Reader, int64(size)); err != nil {
		s.T().Fatalf("seed write: %v", err)
	}
	s.Require().NoError(f.Sync())
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

// readAndDigest reads path from the FUSE mount in 256 KiB chunks and returns
// the total byte count and SHA-256 digest of what was read.
func (s *StreamingReadE2ESuite) readAndDigest(path string) (int64, [sha256.Size]byte) {
	s.T().Helper()
	f, err := os.Open(path)
	s.Require().NoError(err)
	defer f.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, 256<<10))
	s.Require().NoError(err)
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return n, digest
}

// TestRead16MiB writes a 16 MiB known-pattern file server-side and reads it
// back through the FUSE mount. Pre-Phase-3 this would have failed with
// ResourceExhausted at the 4 MiB unary cap.
func (s *StreamingReadE2ESuite) TestRead16MiB() {
	const size = 16 << 20
	src := filepath.Join(s.volume.GetSrcPath(), "stream16.bin")
	want := s.seedRandom(src, size)

	got, gotDigest := s.readAndDigest(filepath.Join(s.volume.GetMountPath(), "stream16.bin"))
	s.Require().Equal(int64(size), got, "short read through FUSE mount")
	s.Require().Equal(want, gotDigest, "payload mismatch through FUSE mount")
}

func TestStreamingReadE2ESuite(t *testing.T) {
	suite.Run(t, new(StreamingReadE2ESuite))
}

// StreamingWriteE2ESuite exercises the client-streaming Write path through a
// real FUSE mount. Pre-Phase-3 these would have hit the 4 MiB unary ceiling
// with ResourceExhausted.
type StreamingWriteE2ESuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
}

func (s *StreamingWriteE2ESuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.testAppCtx = ctx
	s.volume = ctx.GetVolumes()[0]
	ctx.MountVolume(s.volume)
}

func (s *StreamingWriteE2ESuite) TearDownSuite() {
	if s.testAppCtx != nil {
		_ = s.testAppCtx.Close()
	}
	if s.volume != nil {
		_ = s.volume.Close()
	}
}

// writeFromReader streams size bytes from src into a freshly created file at
// path through the FUSE mount in 256 KiB chunks. Returns the SHA-256 digest
// of what was written.
func (s *StreamingWriteE2ESuite) writeFromReader(path string, src io.Reader, size int64) [sha256.Size]byte {
	s.T().Helper()
	f, err := os.Create(path)
	s.Require().NoError(err)
	defer f.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(f, h), io.LimitReader(src, size), make([]byte, 256<<10))
	s.Require().NoError(err)
	s.Require().Equal(size, n, "short write through FUSE mount")
	s.Require().NoError(f.Sync())
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

// digestServerSide hashes the contents of path on the underlying filesystem
// (bypassing FUSE), used to confirm bytes landed server-side.
func (s *StreamingWriteE2ESuite) digestServerSide(path string) (int64, [sha256.Size]byte) {
	s.T().Helper()
	f, err := os.Open(path)
	s.Require().NoError(err)
	defer f.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, 256<<10))
	s.Require().NoError(err)
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return n, digest
}

// TestWrite16MiB writes a 16 MiB random file through the FUSE mount and
// verifies the server-side contents byte-for-byte. Pre-Phase-3 this would
// have failed with ResourceExhausted at the 4 MiB unary cap.
func (s *StreamingWriteE2ESuite) TestWrite16MiB() {
	const size = 16 << 20
	mountPath := filepath.Join(s.volume.GetMountPath(), "write16.bin")
	want := s.writeFromReader(mountPath, rand.Reader, size)

	srcPath := filepath.Join(s.volume.GetSrcPath(), "write16.bin")
	got, gotDigest := s.digestServerSide(srcPath)
	s.Require().Equal(int64(size), got, "short file server-side")
	s.Require().Equal(want, gotDigest, "payload mismatch server-side")
}

// TestBidirectional1GiB is the Phase 3 DoD bidirectional test: write a large
// random file via mount tracking SHA-256, then read it back via mount and
// confirm the hash. The plan calls for 4 GiB but the kubevirt VM has only
// 3.8 GiB RAM; 1 GiB exercises the streaming path comfortably without an
// OOM-killer risk. Task 7 of the Phase 3 plan revisits sizing.
func (s *StreamingWriteE2ESuite) TestBidirectional1GiB() {
	const size = 1 << 30
	mountPath := filepath.Join(s.volume.GetMountPath(), "bidir.bin")
	want := s.writeFromReader(mountPath, rand.Reader, size)

	f, err := os.Open(mountPath)
	s.Require().NoError(err)
	defer f.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, 256<<10))
	s.Require().NoError(err)
	s.Require().Equal(int64(size), n, "short read-back through FUSE mount")
	var got [sha256.Size]byte
	copy(got[:], h.Sum(nil))
	s.Require().Equal(want, got, "read-back hash mismatch")
}

func TestStreamingWriteE2ESuite(t *testing.T) {
	suite.Run(t, new(StreamingWriteE2ESuite))
}
