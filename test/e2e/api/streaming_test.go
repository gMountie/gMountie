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
