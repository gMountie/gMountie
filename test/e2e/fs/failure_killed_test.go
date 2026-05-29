package fs

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// ServerKilledSuite verifies the mount remains well-behaved when the server
// process is killed mid-transfer:
//   - In-flight reads surface an error (EIO-class) rather than hanging or
//     panicking.
//   - In-flight writes surface an error (or block up to a bounded deadline)
//     rather than hanging silently.
//   - After a fresh StartServer the mount self-heals via session recovery:
//     subsequent ops round-trip correctly.
//
// Skip guard: SetupSuite uses MountVolumeErr so that a FUSE-unavailable
// environment (CI sandbox, GoLand, non-Linux) skips cleanly instead of
// panicking. These tests MUST run on the kubevirt VM where fusermount3 is
// present and the ubuntu user can FUSE-mount without sudo.
type ServerKilledSuite struct {
	suite.Suite
	ctx    *utils.AppTestingContext
	volume *utils.TestVolume
}

// fileSize is the transfer size used to make kills land mid-stream
// deterministically. 64 MiB on loopback at default frame/readahead settings
// is long enough that StopServer, fired after the first read returns, will
// always interrupt the remaining stream before it finishes.
const fileSize = 64 * 1024 * 1024 // 64 MiB

func (s *ServerKilledSuite) SetupSuite() {
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

	// MountVolumeErr instead of MountVolume: skip cleanly if FUSE is
	// unavailable (sandbox, GoLand) rather than panicking.
	if err := s.ctx.MountVolumeErr(s.volume); err != nil {
		// Can't unmount, just close and skip.
		_ = testAppCtx.Close()
		s.ctx = nil
		s.T().Skipf("FUSE mount unavailable (not on VM?): %v", err)
	}
}

func (s *ServerKilledSuite) TearDownSuite() {
	// Guard: ctx may be nil when SetupSuite skipped before MountVolume.
	if s.ctx == nil {
		return
	}
	if err := s.ctx.Close(); err != nil {
		s.T().Logf("TearDownSuite: app context close error: %v", err)
	}
}

// seedFile writes n pseudo-random bytes to path (server-side src) and returns
// the SHA-256 digest for later verification. crypto/rand is used so the
// content is not compressible and all bytes are distinct.
func seedFile(path string, n int) ([]byte, error) {
	buf := make([]byte, 64*1024)
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	h := sha256.New()
	remaining := n
	for remaining > 0 {
		sz := len(buf)
		if remaining < sz {
			sz = remaining
		}
		if _, err := rand.Read(buf[:sz]); err != nil {
			return nil, err
		}
		if _, err := f.Write(buf[:sz]); err != nil {
			return nil, err
		}
		h.Write(buf[:sz])
		remaining -= sz
	}
	return h.Sum(nil), nil
}

// TestKilledMidReadSurfacesError opens a 64 MiB file through the mount, starts
// reading it with a small buffer so many gRPC round-trips are needed, signals
// after the first read returns (proving the stream is underway), then kills the
// server. The read goroutine must return an error within 10 s — a hang means
// the client is stuck forever which is the bug this test guards against.
//
// After the server is back up the mount must self-heal: a fresh os.ReadFile of
// the same path must succeed and its SHA-256 must match the seeded digest.
func (s *ServerKilledSuite) TestKilledMidReadSurfacesError() {
	// Seed the file on the server side so it exists before the mount sees it.
	srcPath := filepath.Join(s.volume.GetSrcPath(), "big-read.bin")
	wantSHA, err := seedFile(srcPath, fileSize)
	s.Require().NoError(err)
	defer os.Remove(srcPath) //nolint:errcheck

	mountPath := filepath.Join(s.volume.GetMountPath(), "big-read.bin")

	// T1 gap guard: Close() drains serveErrCh and requires the server to be
	// running when called. Ensure StartServer is called even on FailNow.
	serverRestarted := false
	defer func() {
		if !serverRestarted {
			_ = s.ctx.StartServer()
		}
	}()

	// Open file and start reading in a goroutine with a small buffer (4 KiB)
	// so that the transfer spans many round-trips and the kill can land
	// while the stream is still active.
	f, err := os.Open(mountPath)
	s.Require().NoError(err)

	firstReadDone := make(chan struct{})
	type readResult struct {
		err error
	}
	readDone := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 4*1024) // 4 KiB per read — forces many round-trips over 64 MiB
		firstRead := true
		var readErr error
		for {
			_, err := f.Read(buf)
			if firstRead {
				close(firstReadDone) // signal: first read returned, stream is underway
				firstRead = false
			}
			if err != nil {
				if err == io.EOF {
					// Completed before the kill — this is unlikely at 64 MiB but
					// possible on a very fast machine. Report a nil error; the kill
					// guard below will note that the read finished early.
					readErr = nil
				} else {
					readErr = err
				}
				break
			}
		}
		_ = f.Close()
		readDone <- readResult{err: readErr}
	}()

	// Wait until the stream is definitely underway, then kill the server.
	select {
	case <-firstReadDone:
	case <-time.After(10 * time.Second):
		s.FailNow("timed out waiting for first read to return — mount may be unresponsive")
	}
	s.ctx.KillServer()

	// The key property: the read goroutine must return within 10 s.
	// A hang here is the regression we guard against.
	select {
	case res := <-readDone:
		if res.err == nil {
			// Read completed before the kill. The file finished transferring
			// before StopServer arrived. This is acceptable (not a bug) but
			// log it so the operator can increase fileSize if determinism is
			// needed.
			s.T().Log("WARNING: read completed before server kill; increase fileSize for tighter coverage")
		} else {
			// Got an error — exactly what we expect (EIO-class).
			s.T().Logf("mid-read error (expected): %v", res.err)
		}
	case <-time.After(10 * time.Second):
		s.FailNow("read goroutine did not return within 10 s after server kill — client is hung")
	}

	// Restart the server and verify the mount self-heals.
	s.Require().NoError(s.ctx.StartServer())
	serverRestarted = true

	// Post-restart read: allow a bounded retry window because the client's
	// session recovery may be async relative to the server's health probe.
	var gotSHA []byte
	s.Require().Eventually(func() bool {
		data, readErr := os.ReadFile(mountPath)
		if readErr != nil {
			s.T().Logf("post-restart ReadFile not yet available: %v", readErr)
			return false
		}
		h := sha256.Sum256(data)
		gotSHA = h[:]
		return true
	}, 10*time.Second, 200*time.Millisecond,
		"post-restart ReadFile did not succeed within 10 s — mount did not self-heal")

	s.Require().True(bytes.Equal(wantSHA, gotSHA),
		"post-restart content SHA-256 mismatch — data corruption after server kill+restart")
}

// TestKilledMidWriteSurfacesError starts writing a 64 MiB file through the
// mount in a goroutine with a small buffer (so the transfer spans many gRPC
// round-trips), signals after the first chunk is written, then kills the
// server. The write goroutine must return within a bounded time (10 s) — a
// hang is the regression we guard against.
//
// Must-hold property: no hang or panic; the mount is usable after restart.
//
// Note on error assertion: client-side write buffering / gRPC retry can
// absorb some failed writes without surfacing an error immediately. Rather
// than racing to catch the exact moment the error bubbles up (which is fragile
// and kernel/buffer dependent), we assert the disjunction:
//
//	writeErr != nil || syncErr != nil || closeErr != nil
//
// ...OR, if none surface, the post-restart fresh write+read round-trips
// cleanly. The must-hold property is "no hang, and the mount is usable again
// after restart." This is documented in the plan (Task 3) as the accepted
// weaker property when mid-write error delivery timing is indeterminate.
func (s *ServerKilledSuite) TestKilledMidWriteSurfacesError() {
	dstMountPath := filepath.Join(s.volume.GetMountPath(), "big-write.bin")

	// T1 gap guard: ensure StartServer is called even on FailNow so that
	// TearDownSuite's Close() does not deadlock on serveErrCh.
	serverRestarted := false
	defer func() {
		if !serverRestarted {
			_ = s.ctx.StartServer()
		}
	}()

	firstWriteDone := make(chan struct{})
	type writeResult struct {
		writeErr error
		syncErr  error
		closeErr error
	}
	writeDone := make(chan writeResult, 1)
	go func() {
		f, err := os.OpenFile(dstMountPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			writeDone <- writeResult{writeErr: err}
			close(firstWriteDone)
			return
		}
		buf := make([]byte, 4*1024) // 4 KiB per write — forces many round-trips
		firstWrite := true
		var writeErr error
		remaining := fileSize
		for remaining > 0 && writeErr == nil {
			sz := len(buf)
			if remaining < sz {
				sz = remaining
			}
			if _, err := rand.Read(buf[:sz]); err != nil {
				writeDone <- writeResult{writeErr: err}
				return
			}
			_, writeErr = f.Write(buf[:sz])
			if firstWrite {
				close(firstWriteDone)
				firstWrite = false
			}
			remaining -= sz
		}
		syncErr := f.Sync()
		closeErr := f.Close()
		writeDone <- writeResult{writeErr: writeErr, syncErr: syncErr, closeErr: closeErr}
	}()

	// Wait until the write stream is underway.
	select {
	case <-firstWriteDone:
	case <-time.After(10 * time.Second):
		s.FailNow("timed out waiting for first write to return")
	}
	s.ctx.KillServer()

	// The write goroutine must return within 10 s; a hang is a hard failure.
	var res writeResult
	select {
	case res = <-writeDone:
	case <-time.After(10 * time.Second):
		s.FailNow("write goroutine did not return within 10 s after server kill — client is hung")
	}

	s.T().Logf("write=%v sync=%v close=%v", res.writeErr, res.syncErr, res.closeErr)
	anyErr := res.writeErr != nil || res.syncErr != nil || res.closeErr != nil
	if !anyErr {
		// No error surfaced — acceptable under heavy client-side buffering
		// (see docstring). The must-hold property is no-hang + usable-after-restart.
		// Log explicitly so the result is visible in the test output.
		s.T().Log("NOTE: no write/sync/close error surfaced mid-kill " +
			"(client buffering absorbed the failure); asserting post-restart usability")
	}

	// Restart and verify the mount is usable.
	s.Require().NoError(s.ctx.StartServer())
	serverRestarted = true

	// Clean up any partial file from the aborted write.
	// Ignore error — may not exist if the write never flushed.
	_ = os.Remove(filepath.Join(s.volume.GetSrcPath(), "big-write.bin"))

	// Fresh write+read round-trip through the mount.
	freshPath := filepath.Join(s.volume.GetMountPath(), "post-restart-write.bin")
	want := bytes.Repeat([]byte("R"), 1024*1024) // 1 MiB, simple pattern
	s.Require().Eventually(func() bool {
		err := os.WriteFile(freshPath, want, 0o644)
		if err != nil {
			s.T().Logf("post-restart WriteFile not yet available: %v", err)
			return false
		}
		return true
	}, 10*time.Second, 200*time.Millisecond,
		"post-restart WriteFile did not succeed within 10 s — mount did not self-heal")

	got, err := os.ReadFile(freshPath)
	s.Require().NoError(err, "post-restart ReadFile must succeed")
	s.Require().True(bytes.Equal(want, got),
		"post-restart content mismatch — data corruption after server kill+restart")
	// Clean up.
	_ = os.Remove(filepath.Join(s.volume.GetSrcPath(), "post-restart-write.bin"))
}

func TestServerKilledSuite(t *testing.T) {
	suite.Run(t, new(ServerKilledSuite))
}
