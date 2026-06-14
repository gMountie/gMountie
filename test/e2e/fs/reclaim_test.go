package fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

// reclaimSeedSize is the size of the file seeded for the read-reclaim test.
// 4 MiB spans several readahead chunks so the server-side fd is genuinely open
// when the restart lands, while staying small enough to read to EOF quickly.
const reclaimSeedSize = 4 * 1024 * 1024 // 4 MiB

// reclaimChunkSize is the size of each write chunk in the write-reclaim test.
const reclaimChunkSize = 256 * 1024 // 256 KiB

// ReclaimAcrossRestartSuite validates the session-RECLAIM contract, which is the
// opposite of ReconnectOpenFDSuite. Here the server is genuinely RESTARTED with
// an empty in-memory session table (RestartServerNewSession): the client's
// keepalive recovery loop calls Resume(old_session_id), which FAILS against the
// fresh table, so it falls back to Create and mints a NEW session_id. The client
// must then transparently REOPEN every open file against the new session and
// remap the server fd, so the application's in-flight reads and writes resume
// without error and without data loss.
//
// The two distinguishing assertions vs. the reconnect suite:
//   - sidAfter != sidBefore (a fresh-table restart MUST take the Create path),
//     whereas the reconnect suite asserts the id is UNCHANGED (Resume path).
//   - the write test proves the reopen did NOT re-apply O_TRUNC: bytes written
//     and fsync'd before the restart survive (the sanitizeReopenFlags guard).
//
// Env gate: like the reconnect suite, this holds an fd open across a server
// disruption, so it runs only on the dedicated kubevirt VM (GMOUNTIE_E2E_VM=1).
// CI runners DO have /dev/fuse, so a "mount fails => skip" guard is not enough:
// a stranded in-flight FUSE request could hang teardown until the job timeout.
type ReclaimAcrossRestartSuite struct {
	suite.Suite
	ctx      *utils.AppTestingContext
	volume   *utils.TestVolume
	readRPCs atomic.Int64 // counts server-side RpcFile/Read streaming calls
}

// countingReadInterceptor increments readRPCs each time a Read RPC reaches the
// server. The read-reclaim test uses it to prove the post-restart tail read
// actually traversed the reopened server-side fd rather than being served from
// the FUSE kernel page cache (readahead can prefetch the tail before the
// restart, which would otherwise let the SHA assertion pass even if read
// reclaim were broken). The interceptor persists across RestartServerNewSession:
// buildServer reuses c.serverOptions, so the same closure and atomic counter
// survive the rebuild.
func (s *ReclaimAcrossRestartSuite) countingReadInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == "/gmountie.RpcFile/Read" {
			s.readRPCs.Add(1)
		}
		return handler(srv, ss)
	}
}

func (s *ReclaimAcrossRestartSuite) SetupSuite() {
	// VM-only gate (MUST be the first statement, before any server/mount setup).
	if os.Getenv("GMOUNTIE_E2E_VM") != "1" {
		s.T().Skip("GMOUNTIE_E2E_VM not set to 1; skipping VM-only reclaim suite")
	}
	// A real TCP transport is required so StartServer can rebind the same
	// address after the restart (bufconn cannot rebind). No proxy is needed:
	// we restart the server process rather than sever a connection. The
	// passthrough volume mounts writable as root, which both tests need.
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithTCPTransport(),
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithServerStreamInterceptors(s.countingReadInterceptor()),
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

func (s *ReclaimAcrossRestartSuite) TearDownSuite() {
	if s.ctx == nil {
		return
	}
	if err := s.ctx.Close(); err != nil {
		s.T().Logf("TearDownSuite: close error: %v", err)
	}
}

// waitForNewSession restarts the server with a fresh, empty session table and
// blocks until the client has both recovered its transport (a Version RPC
// round-trips) AND minted a NEW session id. The client's keepalive recovery
// loop performs the Resume→Create fallback EAGERLY in the background (see
// SessionHandshake.keepaliveLoop / tryReattach), so the id flips on its own
// without an FS op; folding the id check into the same poll removes the race
// where Version() recovers a beat before the background Create completes.
//
// It returns the new session id and asserts it differs from sidBefore — THIS is
// the key divergence from the reconnect suite (which asserts the id is stable).
func (s *ReclaimAcrossRestartSuite) waitForNewSession(sidBefore string) string {
	s.Require().NoError(s.ctx.RestartServerNewSession(),
		"fresh-table restart must rebind and become healthy")

	var sidAfter string
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, verr := s.ctx.GetClient().Version().Get(ctx, &proto.VersionRequest{}); verr != nil {
			s.T().Logf("recovery probe (version): %v", verr)
			return false
		}
		sidAfter = s.ctx.GetClient().SessionID()
		return sidAfter != "" && sidAfter != sidBefore
	}, 10*time.Second, 100*time.Millisecond,
		"client did not recover with a NEW session id within 10 s after fresh-table restart")

	s.Require().NotEqual(sidBefore, sidAfter,
		"a fresh-table restart must mint a NEW session id (Create path)")
	return sidAfter
}

// TestReadFDSurvivesServerRestart seeds a 4 MiB file, opens it through the mount,
// reads the first 1 MiB (establishing a server-side fd), then restarts the
// server with an EMPTY session table. The client recovers via Create (new id),
// and reclaim must reopen the read fd against the new session so the SAME
// os.File reads to EOF and the full content matches the seed byte-for-byte.
func (s *ReclaimAcrossRestartSuite) TestReadFDSurvivesServerRestart() {
	// --- Step 1: seed the file server-side ---
	srcPath := filepath.Join(s.volume.GetSrcPath(), "reclaim-read.bin")
	wantSHA, err := seedFile(srcPath, reclaimSeedSize)
	s.Require().NoError(err, "seed file")
	defer os.Remove(srcPath) //nolint:errcheck

	mountPath := filepath.Join(s.volume.GetMountPath(), "reclaim-read.bin")

	// --- Step 2: open through the mount and read the first 1 MiB ---
	// The OS open + read establishes an active server-side fd before the restart.
	f, err := os.Open(mountPath)
	s.Require().NoError(err, "open file through mount")
	defer f.Close() //nolint:errcheck

	chunk0 := make([]byte, 1*1024*1024) // 1 MiB
	_, err = io.ReadFull(f, chunk0)
	s.Require().NoError(err, "read first 1 MiB chunk through mount")

	// Snapshot the Read RPC count after the first chunk. Any increase past this
	// point must come from post-restart server-side Reads over the reopened fd.
	readsBefore := s.readRPCs.Load()

	// --- Step 3: capture session id before the restart ---
	sidBefore := s.ctx.GetClient().SessionID()
	s.Require().NotEmpty(sidBefore, "session id must be non-empty before restart")

	// --- Step 4 + 5 + 6: restart with empty table, wait for recovery + NEW id ---
	sidAfter := s.waitForNewSession(sidBefore)

	// --- Step 7: continue reading the SAME open fd to EOF ---
	// Read in a goroutine with a bounded deadline so a wedged (un-reclaimed) fd
	// produces a clear failure rather than hanging the suite.
	type readResult struct {
		data []byte
		err  error
	}
	readDone := make(chan readResult, 1)
	go func() {
		rest, rerr := io.ReadAll(f)
		readDone <- readResult{data: rest, err: rerr}
	}()

	var rest []byte
	select {
	case res := <-readDone:
		if res.err != nil {
			s.Failf("open fd read failed after fresh-table restart (reclaim violated)",
				"error: %v  sidBefore=%s sidAfter=%s",
				res.err, sidBefore, sidAfter)
			return
		}
		rest = res.data
	case <-time.After(20 * time.Second):
		s.FailNow("read-to-EOF timed out after 20 s — read fd appears un-reclaimed after restart")
		return
	}

	// --- Assert the tail actually hit the server, not just the page cache ---
	// FUSE kernel readahead can prefetch the tail before the restart; without
	// this check the SHA assertion below could pass from cached bytes even if
	// read reclaim were broken. A post-restart Read RPC proves the reopened
	// server-side fd was genuinely exercised.
	readsAfter := s.readRPCs.Load()
	s.Require().Greater(readsAfter, readsBefore,
		"server-side Read RPCs must have increased after the restart: "+
			"the tail must traverse the reclaimed fd, not arrive from the kernel page cache")

	// --- Verify content integrity: reclaim reopened the read fd correctly ---
	full := append(chunk0, rest...)
	s.Require().Len(full, reclaimSeedSize,
		"total bytes read must equal the seeded file size")
	gotSHA := sha256.Sum256(full)
	s.Require().True(bytes.Equal(wantSHA, gotSHA[:]),
		"SHA-256 of bytes read across the fresh-table restart must match the seeded content")
	s.T().Logf("read fd reclaimed: %d bytes, sha256 OK, session %s → %s, server Read RPCs %d → %d",
		len(full), sidBefore, sidAfter, readsBefore, readsAfter)
}

// TestWriteFDSurvivesServerRestartNoTruncation is the data-loss guard. It creates
// a file through the mount with O_TRUNC, writes+fsyncs chunk1 (pattern A) so it
// is durable, then restarts the server with an EMPTY session table. Through the
// SAME fd it writes chunk2 (pattern B). Reading the file back must yield exactly
// chunk1||chunk2, proving:
//
//	(a) reclaim reopened the WRITE fd against the new session,
//	(b) the reopen did NOT re-apply O_TRUNC (chunk1 survived — the
//	    sanitizeReopenFlags guard), and
//	(c) post-restart writes land on the server.
func (s *ReclaimAcrossRestartSuite) TestWriteFDSurvivesServerRestartNoTruncation() {
	mountPath := filepath.Join(s.volume.GetMountPath(), "reclaim-write.bin")
	srcPath := filepath.Join(s.volume.GetSrcPath(), "reclaim-write.bin")
	defer os.Remove(srcPath) //nolint:errcheck

	chunk1 := bytes.Repeat([]byte{0xA1}, reclaimChunkSize) // pattern A
	chunk2 := bytes.Repeat([]byte{0xB2}, reclaimChunkSize) // pattern B

	// --- Step 1: create the file through the mount with O_TRUNC ---
	f, err := os.OpenFile(mountPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	s.Require().NoError(err, "create file through mount")
	defer f.Close() //nolint:errcheck

	// --- Step 2: write + fsync chunk1 so it is durable BEFORE the restart ---
	n, err := f.Write(chunk1)
	s.Require().NoError(err, "write chunk1")
	s.Require().Equal(reclaimChunkSize, n, "short write on chunk1")
	s.Require().NoError(f.Sync(), "fsync chunk1 before restart")

	// --- Step 3: capture session id before the restart ---
	sidBefore := s.ctx.GetClient().SessionID()
	s.Require().NotEmpty(sidBefore, "session id must be non-empty before restart")

	// --- Step 4 + 5: restart with empty table, wait for recovery + NEW id ---
	sidAfter := s.waitForNewSession(sidBefore)

	// --- Step 5 (write): write chunk2 through the SAME fd in a bounded goroutine ---
	// A wedged (un-reclaimed) write fd would hang here; the timeout converts that
	// into a clear failure instead of a suite hang.
	type writeResult struct{ err error }
	writeDone := make(chan writeResult, 1)
	go func() {
		if _, werr := f.Write(chunk2); werr != nil {
			writeDone <- writeResult{err: werr}
			return
		}
		if serr := f.Sync(); serr != nil {
			writeDone <- writeResult{err: serr}
			return
		}
		writeDone <- writeResult{err: f.Close()}
	}()

	select {
	case res := <-writeDone:
		if res.err != nil {
			s.Failf("write through fd failed after fresh-table restart (reclaim violated)",
				"error: %v  sidBefore=%s sidAfter=%s",
				res.err, sidBefore, sidAfter)
			return
		}
	case <-time.After(20 * time.Second):
		s.FailNow("write-after-restart timed out after 20 s — write fd appears un-reclaimed")
		return
	}

	// --- Step 6: read the file back from the server-side src and verify ---
	// Reading the backing file directly avoids any client-side cache and proves
	// the bytes actually landed on disk.
	got, err := os.ReadFile(srcPath)
	s.Require().NoError(err, "read back the server-side file")

	want := append(append([]byte{}, chunk1...), chunk2...)
	s.Require().Len(got, 2*reclaimChunkSize,
		"file must be chunk1||chunk2 (512 KiB): chunk1 not truncated, chunk2 landed")
	s.Require().True(bytes.Equal(want, got),
		"content must equal chunk1||chunk2 byte-for-byte: "+
			"reclaim reopened the write fd WITHOUT re-applying O_TRUNC and the post-restart write landed")
	s.T().Logf("write fd reclaimed: %d bytes (chunk1 survived, chunk2 landed), session %s → %s",
		len(got), sidBefore, sidAfter)
}

func TestReclaimAcrossRestart(t *testing.T) {
	suite.Run(t, new(ReclaimAcrossRestartSuite))
}
