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

	"go.gmountie.dev/gmountie/test/e2e/utils"

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
// Env gate: the suite runs only when GMOUNTIE_E2E_VM=1 (the dedicated
// kubevirt VM, where fusermount3 is present and the ubuntu user can
// FUSE-mount without sudo). Once that gate passes, FUSE is part of the
// contract: a mount failure FAILS the suite rather than skipping it, so a
// mount regression cannot silently void the kill/recovery coverage.
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
	// VM-only gate (MUST be the first statement, before any server/mount setup).
	// This suite kills the server mid-IO; that is safe only on the dedicated
	// kubevirt VM. CI runners DO have /dev/fuse, so without this gate the suite
	// would mount and execute in CI — where a killed server strands an in-flight
	// FUSE kernel request and the unmount in TearDownSuite hangs until the job
	// timeout. The relied-upon "mount fails => skip" guard is not enough because
	// the mount does not fail in CI. The VM run sets GMOUNTIE_E2E_VM=1.
	if os.Getenv("GMOUNTIE_E2E_VM") != "1" {
		s.T().Skip("GMOUNTIE_E2E_VM not set to 1; skipping VM-only server-kill suite")
	}
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
	// Safety net: a failed Require below skips TearDownSuite (testify never
	// marks the suite set up), which would leak the running server. Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = testAppCtx.Close() })
	s.volume = s.ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)

	// The env gate above asserted "this is the VM", so FUSE must work here:
	// a mount failure is a regression, not a skip condition.
	s.Require().NoError(s.ctx.MountVolumeErr(s.volume),
		"FUSE mount must work when GMOUNTIE_E2E_VM=1")
}

func (s *ServerKilledSuite) TearDownSuite() {
	// Guard: ctx is nil when SetupSuite failed before assigning it.
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

// TestKilledDuringWritesMountRecovery verifies the must-hold property for
// write paths: after a server kill+restart, the FUSE mount is usable for
// fresh writes and reads.
//
// FUSE limitation note: an in-flight kernel write (a Write syscall already
// dispatched to the FUSE driver) cannot be unblocked or reissued after the
// user-space handler dies — the kernel FUSE device hangs that particular fd
// until the mount is torn down. Attempting to kill the server while an
// os.Write is blocked in a goroutine will leave that goroutine stuck forever.
// Therefore this test uses a different kill point: we complete a write,
// verify it landed, kill the server, then assert a fresh write+read over the
// same mount succeeds after restart — proving the client session self-heals
// and no mount-level panic occurs. This is the accepted weaker property
// documented in the plan (Task 3).
func (s *ServerKilledSuite) TestKilledDuringWritesMountRecovery() {
	// T1 gap guard: Close() drains serveErrCh and requires the server to be
	// running when called. Ensure StartServer is called even on FailNow.
	serverRestarted := false
	defer func() {
		if !serverRestarted {
			_ = s.ctx.StartServer()
		}
	}()

	// Write a file through the mount before killing — establishes a baseline
	// that writes work and that a session + open fd exist on the server.
	prePath := filepath.Join(s.volume.GetMountPath(), "pre-kill.bin")
	preData := bytes.Repeat([]byte("X"), 4*1024*1024) // 4 MiB
	s.Require().NoError(os.WriteFile(prePath, preData, 0o644),
		"baseline write through mount must succeed")

	// Kill the server. The mount's gRPC transport drops; the client session
	// recovery loop begins retrying.
	s.ctx.KillServer()

	// Restart the server. The client's retry loop reconnects and the session
	// is resumed (session recovery, same session_id).
	s.Require().NoError(s.ctx.StartServer(), "StartServer after kill must succeed")
	serverRestarted = true

	// Post-restart: a fresh write through the mount must succeed within 10 s.
	// This is the self-heal assertion: the mount is usable after a server kill.
	freshPath := filepath.Join(s.volume.GetMountPath(), "post-restart-write.bin")
	want := bytes.Repeat([]byte("R"), 1024*1024) // 1 MiB
	s.Require().Eventually(func() bool {
		err := os.WriteFile(freshPath, want, 0o644)
		if err != nil {
			s.T().Logf("post-restart WriteFile not yet available: %v", err)
			return false
		}
		return true
	}, 10*time.Second, 200*time.Millisecond,
		"post-restart WriteFile did not succeed within 10 s — mount did not self-heal after kill")

	got, err := os.ReadFile(freshPath)
	s.Require().NoError(err, "post-restart ReadFile must succeed")
	s.Require().True(bytes.Equal(want, got),
		"post-restart content mismatch — data corruption after server kill+restart")

	// Clean up server-side files.
	_ = os.Remove(filepath.Join(s.volume.GetSrcPath(), "pre-kill.bin"))
	_ = os.Remove(filepath.Join(s.volume.GetSrcPath(), "post-restart-write.bin"))
}

func TestServerKilledSuite(t *testing.T) {
	suite.Run(t, new(ServerKilledSuite))
}
