package api

import (
	"crypto/rand"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
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
	// Safety net: a failed Require below skips TearDownSuite; Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.volume = ctx.GetVolumes()[0]
	s.Require().NoError(ctx.MountVolumeErr(s.volume))
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
	// Safety net: a failed Require below skips TearDownSuite; Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.volume = ctx.GetVolumes()[0]
	s.Require().NoError(ctx.MountVolumeErr(s.volume))
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

// TestBidirectionalLargeRoundTrip is the Phase 3 DoD bidirectional test:
// write a large random file via mount tracking SHA-256, then read it back via
// mount and confirm the hash. CI runs a 128 MiB payload — plenty to exercise
// the streaming frame path many times over while staying inside the CI test
// budget. The full 1 GiB variant (the original Phase 3 sizing, capped from
// 4 GiB for the kubevirt VM's 3.8 GiB RAM) runs only under
// GMOUNTIE_E2E_LARGEFILE=1, the same gate as the fs LargeFileSuite.
func (s *StreamingWriteE2ESuite) TestBidirectionalLargeRoundTrip() {
	size := int64(128 << 20) // CI default
	if os.Getenv("GMOUNTIE_E2E_LARGEFILE") == "1" {
		size = 1 << 30
	}
	mountPath := filepath.Join(s.volume.GetMountPath(), "bidir.bin")
	want := s.writeFromReader(mountPath, rand.Reader, size)

	f, err := os.Open(mountPath)
	s.Require().NoError(err)
	defer f.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, 256<<10))
	s.Require().NoError(err)
	s.Require().Equal(size, n, "short read-back through FUSE mount")
	var got [sha256.Size]byte
	copy(got[:], h.Sum(nil))
	s.Require().Equal(want, got, "read-back hash mismatch")
}

func TestStreamingWriteE2ESuite(t *testing.T) {
	suite.Run(t, new(StreamingWriteE2ESuite))
}

// WriteCoalesceE2ESuite verifies that the client-side per-fd write
// coalescer batches many small contiguous writes into far fewer streaming
// Write RPCs against the server. The suite installs a stream interceptor
// on the test gRPC server that counts RpcFile/Write streams so the test
// can assert on observed RPC volume in addition to byte-for-byte content.
type WriteCoalesceE2ESuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
	writeRPCs  atomic.Int64
}

func (s *WriteCoalesceE2ESuite) countingWriteInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == "/gmountie.RpcFile/Write" {
			s.writeRPCs.Add(1)
		}
		return handler(srv, ss)
	}
}

func (s *WriteCoalesceE2ESuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithServerStreamInterceptors(s.countingWriteInterceptor()),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.testAppCtx = ctx
	// Safety net: a failed Require below skips TearDownSuite; Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.volume = ctx.GetVolumes()[0]
	s.Require().NoError(ctx.MountVolumeErr(s.volume))
}

func (s *WriteCoalesceE2ESuite) TearDownSuite() {
	if s.testAppCtx != nil {
		_ = s.testAppCtx.Close()
	}
	if s.volume != nil {
		_ = s.volume.Close()
	}
}

// TestManySmallWritesCoalesce performs 4096 sequential 8-byte writes (32 KiB
// total) through the FUSE mount and asserts:
//   - the server-side file content matches what was written byte-for-byte,
//   - the number of observed Write RPCs is far below the 4096 small-write
//     count — with the default 1 MiB threshold, 32 KiB of contiguous data
//     should produce a single batched RPC plus at most a few on Flush/Release.
//
// Without coalescing this would issue ~4096 Write streams. We use an
// upper bound of 32 to keep the assertion robust against kernel page-cache
// behaviour and FUSE write coalescing in the kernel, while still being a
// strong signal that the client coalescer is doing meaningful work.
func (s *WriteCoalesceE2ESuite) TestManySmallWritesCoalesce() {
	const (
		writeCount = 4096
		writeSize  = 8
	)
	s.writeRPCs.Store(0)

	mountPath := filepath.Join(s.volume.GetMountPath(), "coalesce.bin")
	f, err := os.Create(mountPath)
	s.Require().NoError(err)

	want := make([]byte, 0, writeCount*writeSize)
	chunk := make([]byte, writeSize)
	for i := 0; i < writeCount; i++ {
		// Deterministic per-write content so any out-of-order or dropped
		// bytes show up in the byte-for-byte server-side check.
		for j := range chunk {
			chunk[j] = byte((i + j) % 251)
		}
		n, werr := f.Write(chunk)
		s.Require().NoError(werr)
		s.Require().Equal(writeSize, n)
		want = append(want, chunk...)
	}
	s.Require().NoError(f.Sync())
	s.Require().NoError(f.Close())

	// Verify server-side content.
	srcPath := filepath.Join(s.volume.GetSrcPath(), "coalesce.bin")
	got, err := os.ReadFile(srcPath)
	s.Require().NoError(err)
	s.Require().Len(got, len(want), "short file server-side")
	s.Require().Equal(sha256.Sum256(want), sha256.Sum256(got), "payload mismatch server-side")

	// Verify the client coalesced the small writes into far fewer RPCs.
	// Without coalescing this would be ~4096; with a 1 MiB threshold and
	// 32 KiB total, we expect on the order of 1.
	observed := s.writeRPCs.Load()
	s.T().Logf("observed %d Write RPCs for %d small writes (%d bytes)", observed, writeCount, writeCount*writeSize)
	s.Assert().Less(observed, int64(32),
		"expected coalescer to reduce 4096 small writes to <32 Write RPCs, got %d", observed)
}

func TestWriteCoalesceE2ESuite(t *testing.T) {
	suite.Run(t, new(WriteCoalesceE2ESuite))
}
