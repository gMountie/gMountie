package fs

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

const (
	// largeFileDefault is the default file size: 4 GiB. This crosses the
	// int32/uint32 boundary in Read/Write/Seek offset plumbing and is the
	// primary regression target of this suite.
	largeFileDefault = 4 * 1024 * 1024 * 1024 // 4 GiB

	// largeFileSeed is the fixed PRNG seed used for the deterministic byte
	// stream. A fixed seed means the write-side and the tail-verification
	// reader can both regenerate the same stream independently without
	// holding the full file in memory.
	largeFileSeed int64 = 0x1EADBEEF_CAFEBAB

	// largeCopyBuf is the io.CopyBuffer chunk size. 4 MiB is large enough to
	// keep the FUSE + gRPC pipeline busy but small enough not to pressure the
	// VM's available memory.
	largeCopyBuf = 4 * 1024 * 1024 // 4 MiB

	// largeTailSize is the number of bytes verified in the targeted large-offset
	// read. 4 KiB is sufficient to span at least one FUSE block and catch any
	// 32-bit truncation of the seek offset.
	largeTailSize = 4096
)

// LargeFileSuite verifies that a file larger than 4 GiB can be written to and
// read back through the FUSE mount with byte-perfect integrity.
//
// Env gates:
//   - GMOUNTIE_E2E_LARGEFILE=1   — must be set, otherwise the suite skips.
//   - GMOUNTIE_E2E_LARGEFILE_SIZE — size in bytes (optional, default 4 GiB).
//
// This suite MUST run on the kubevirt VM where fusermount3 is present and the
// ubuntu user can FUSE-mount without sudo.  It is intentionally omitted from
// CI by the env gate because writing 4 GiB is slow and disk-heavy.
type LargeFileSuite struct {
	suite.Suite
	ctx      *utils.AppTestingContext
	volume   *utils.TestVolume
	fileSize int64
	bigFile  string // abs path of the large file under GetSrcPath()
}

// seededReader is an io.Reader that emits a deterministic byte stream derived
// from a seeded PRNG.  It generates 8 bytes at a time from rand.Int63 and
// feeds them to a running sha256 hash so the caller gets the digest without a
// second pass.  Using math/rand (not crypto/rand) keeps throughput high enough
// to sustain FUSE write bandwidth.
type seededReader struct {
	rng  *rand.Rand
	h    io.Writer // typically sha256.New()
	buf  [8]byte
	frag []byte // leftover bytes from the previous Int63 word
	done int64  // bytes emitted so far
	size int64  // total bytes to emit
}

func newSeededReader(seed int64, size int64, h io.Writer) *seededReader {
	return &seededReader{
		rng:  rand.New(rand.NewSource(seed)), //nolint:gosec // deterministic PRNG, not for security
		h:    h,
		size: size,
	}
}

func (r *seededReader) Read(p []byte) (int, error) {
	if r.done >= r.size {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && r.done < r.size {
		// Refill fragment if needed.
		if len(r.frag) == 0 {
			binary.LittleEndian.PutUint64(r.buf[:], uint64(r.rng.Int63())) //nolint:gosec
			r.frag = r.buf[:]
		}
		// Clamp to remaining file size.
		rem := r.size - r.done
		avail := int64(len(r.frag))
		want := int64(len(p) - n)
		if want > avail {
			want = avail
		}
		if want > rem {
			want = rem
		}
		copy(p[n:], r.frag[:want])
		r.frag = r.frag[want:]
		n += int(want)
		r.done += want
	}
	if r.h != nil {
		// Hash the bytes we just produced.  Never errors for sha256.
		_, _ = r.h.Write(p[:n])
	}
	if r.done >= r.size {
		return n, io.EOF
	}
	return n, nil
}

// largeTailBytes regenerates the last largeTailSize bytes of the deterministic
// stream without reading the whole file.  It advances the PRNG to the correct
// position by consuming bytes in the same order, discarding all but the last
// largeTailSize bytes.
func largeTailBytes(seed int64, size int64) []byte {
	// We need to regenerate bytes [size-largeTailSize, size).
	// seededReader produces 8-byte words; compute the word boundary.
	tailStart := size - largeTailSize
	buf := make([]byte, largeTailSize)
	sr := newSeededReader(seed, size, io.Discard)
	// Discard everything before tailStart in chunked reads so we don't
	// allocate a 4 GiB buffer.
	discard := tailStart
	tmp := make([]byte, 1*1024*1024)
	for discard > 0 {
		chunk := int64(len(tmp))
		if chunk > discard {
			chunk = discard
		}
		n, err := io.ReadFull(sr, tmp[:chunk])
		discard -= int64(n)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			panic(fmt.Sprintf("largeTailBytes: discard read: %v", err))
		}
	}
	// Now read the tail.
	if _, err := io.ReadFull(sr, buf); err != nil {
		panic(fmt.Sprintf("largeTailBytes: tail read: %v", err))
	}
	return buf
}

func (s *LargeFileSuite) SetupSuite() {
	// --- Env gate (checked first: fast skip in CI/sandbox) ---
	if os.Getenv("GMOUNTIE_E2E_LARGEFILE") != "1" {
		s.T().Skip("GMOUNTIE_E2E_LARGEFILE not set to 1; skipping large-file suite (env-gated)")
	}

	// --- Determine file size ---
	s.fileSize = largeFileDefault
	if v := os.Getenv("GMOUNTIE_E2E_LARGEFILE_SIZE"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			s.T().Fatalf("GMOUNTIE_E2E_LARGEFILE_SIZE must be a positive integer (bytes); got %q", v)
		}
		s.fileSize = n
	}
	s.T().Logf("large-file suite: size=%d bytes (%.2f GiB)", s.fileSize, float64(s.fileSize)/(1<<30))

	// --- Build the test harness ---
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithTCPTransport(),
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	if err != nil {
		s.T().Fatal(err)
	}
	if err := testAppCtx.Start(); err != nil {
		s.T().Fatal(err)
	}
	s.ctx = testAppCtx
	s.volume = s.ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)

	// --- FUSE mount (skip guard: skip cleanly if fusermount3 absent) ---
	if err := s.ctx.MountVolumeErr(s.volume); err != nil {
		_ = testAppCtx.Close()
		s.ctx = nil
		s.T().Skipf("FUSE mount unavailable (not on VM?): %v", err)
	}
}

func (s *LargeFileSuite) TearDownSuite() {
	if s.ctx == nil {
		return
	}
	// Remove the big file from the source tree so the VM disk is reclaimed even
	// if the test itself did not reach the cleanup defer.
	if s.bigFile != "" {
		if err := os.Remove(s.bigFile); err != nil && !os.IsNotExist(err) {
			s.T().Logf("TearDownSuite: remove big file: %v", err)
		}
	}
	if err := s.ctx.Close(); err != nil {
		s.T().Logf("TearDownSuite: app context close: %v", err)
	}
}

// TestLargeFileRoundTripIntegrity writes a >= 4 GiB deterministic byte stream
// through the FUSE mount, reads it back, and asserts:
//  1. write size == read size == requested size
//  2. write SHA-256 == read SHA-256   (full-file integrity)
//  3. the last 4 KiB at offset (size - 4096) match the regenerated tail
//     (targeted large-offset seek: directly catches a 32-bit int truncation
//     that a from-zero streaming read might not surface).
func (s *LargeFileSuite) TestLargeFileRoundTripIntegrity() {
	mountPath := filepath.Join(s.volume.GetMountPath(), "largefile-roundtrip.bin")
	srcPath := filepath.Join(s.volume.GetSrcPath(), "largefile-roundtrip.bin")
	s.bigFile = srcPath // register for TearDownSuite cleanup

	// Ensure the file is removed even if the test fails partway through so
	// the VM disk is not left holding a 4 GiB orphan.
	defer func() {
		if err := os.Remove(srcPath); err != nil && !os.IsNotExist(err) {
			s.T().Logf("deferred cleanup: remove srcPath: %v", err)
		}
		// Clear so TearDownSuite does not double-remove.
		s.bigFile = ""
	}()

	// ---- WRITE PHASE ----
	wHash := sha256.New()
	writeReader := newSeededReader(largeFileSeed, s.fileSize, wHash)

	wStart := time.Now()
	dst, err := os.Create(mountPath)
	s.Require().NoError(err, "create file through mount")

	copyBuf := make([]byte, largeCopyBuf)
	written, err := io.CopyBuffer(dst, writeReader, copyBuf)
	s.Require().NoError(err, "write large file through mount")
	s.Require().NoError(dst.Sync(), "fsync large file")
	s.Require().NoError(dst.Close(), "close large file after write")

	wDur := time.Since(wStart)
	writeSHA := wHash.Sum(nil)
	s.Require().Equal(s.fileSize, written,
		"write: bytes written (%d) != requested size (%d)", written, s.fileSize)
	s.T().Logf("write: %d bytes in %s (%.2f MiB/s)",
		written, wDur.Round(time.Second),
		float64(written)/wDur.Seconds()/(1<<20))

	// ---- READ PHASE ----
	rHash := sha256.New()
	rStart := time.Now()
	src, err := os.Open(mountPath)
	s.Require().NoError(err, "open file for read through mount")

	// io.Discard wrapped to feed the hash — never allocates a full file buffer.
	read, err := io.CopyBuffer(rHash, src, copyBuf)
	s.Require().NoError(err, "read large file through mount")
	s.Require().NoError(src.Close(), "close file after read")

	rDur := time.Since(rStart)
	readSHA := rHash.Sum(nil)
	s.Require().Equal(s.fileSize, read,
		"read: bytes read (%d) != requested size (%d)", read, s.fileSize)
	s.T().Logf("read: %d bytes in %s (%.2f MiB/s)",
		read, rDur.Round(time.Second),
		float64(read)/rDur.Seconds()/(1<<20))

	// ---- INTEGRITY ASSERTIONS ----
	s.Require().Equal(writeSHA, readSHA,
		"SHA-256 mismatch: write=%x read=%x — possible chunking/offset corruption", writeSHA, readSHA)
	s.T().Logf("SHA-256 match: %x", readSHA)

	// ---- LARGE-OFFSET SEEK VERIFICATION ----
	// Directly seek to (size - largeTailSize) and read the last 4 KiB.  This
	// exercises the int64 offset path in FUSE Read and gRPC offset plumbing: a
	// 32-bit truncation of the seek offset would land at a wrong position and
	// return mismatched bytes, even if the full streaming read above happened to
	// succeed by reading monotonically from offset 0.
	tailWant := largeTailBytes(largeFileSeed, s.fileSize)

	seekFile, err := os.Open(mountPath)
	s.Require().NoError(err, "open file for tail-seek read")
	defer seekFile.Close() //nolint:errcheck

	seekOff := s.fileSize - largeTailSize
	got, err := seekFile.Seek(seekOff, io.SeekStart)
	s.Require().NoError(err, "seek to large offset %d", seekOff)
	s.Require().Equal(seekOff, got,
		"seek returned wrong position: want %d got %d", seekOff, got)

	tailGot := make([]byte, largeTailSize)
	_, err = io.ReadFull(seekFile, tailGot)
	s.Require().NoError(err, "read tail bytes at large offset")

	s.Require().Equal(tailWant, tailGot,
		"tail-offset read mismatch at offset %d — 32-bit offset truncation suspected", seekOff)
	s.T().Logf("tail-offset check passed: last %d bytes at offset %d match", largeTailSize, seekOff)
}

func TestLargeFileSuite(t *testing.T) {
	suite.Run(t, new(LargeFileSuite))
}
