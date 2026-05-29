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

	"gmountie/pkg/proto"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

// seedSize is the size of the file seeded for reconnect tests. 8 MiB is large
// enough to span multiple readahead chunks and ensure the server-side fd is
// open when the connection drops, but small enough to complete quickly
// well within the 30 s GracePeriod so the session reap timer never fires.
const seedSize = 8 * 1024 * 1024 // 8 MiB

// ReconnectOpenFDSuite validates the Phase 1 session-reclamation contract:
// when the TCP connection drops (server stays up, within GracePeriod), the
// client's keepalive recovery loop calls Resume with the same session_id, the
// server cancels the reap timer, and any open file descriptor on that session
// remains valid. The test asserts the strong contract: the same os.File read
// to EOF after Restore matches the seeded content byte-for-byte AND the tail
// bytes arrive over fresh server-side Read RPCs (not from the kernel page
// cache), proving the surviving fd is actually exercised.
//
// Skip guard: SetupSuite uses MountVolumeErr so that a FUSE-unavailable
// environment (CI sandbox, GoLand, non-Linux) skips cleanly instead of
// panicking. These tests MUST run on the kubevirt VM where fusermount3 is
// present and the ubuntu user can FUSE-mount without sudo.
type ReconnectOpenFDSuite struct {
	suite.Suite
	ctx      *utils.AppTestingContext
	volume   *utils.TestVolume
	readRPCs atomic.Int64 // counts server-side RpcFile/Read streaming calls
}

// countingReadInterceptor is a server stream interceptor that increments
// readRPCs each time a Read RPC arrives at the server. This lets the test
// assert that the post-restore tail read actually traversed the server (not
// the kernel page cache), proving the surviving server-side fd is live.
func (s *ReconnectOpenFDSuite) countingReadInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == "/gmountie.RpcFile/Read" {
			s.readRPCs.Add(1)
		}
		return handler(srv, ss)
	}
}

func (s *ReconnectOpenFDSuite) SetupSuite() {
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithTCPTransport(),
		utils.WithProxy(),
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
	s.volume = s.ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)

	// MountVolumeErr instead of MountVolume: skip cleanly if FUSE is
	// unavailable (sandbox, GoLand) rather than panicking.
	if err := s.ctx.MountVolumeErr(s.volume); err != nil {
		_ = testAppCtx.Close()
		s.ctx = nil
		s.T().Skipf("FUSE mount unavailable (not on VM?): %v", err)
	}
}

func (s *ReconnectOpenFDSuite) TearDownSuite() {
	if s.ctx == nil {
		return
	}
	if err := s.ctx.Close(); err != nil {
		s.T().Logf("TearDownSuite: close error: %v", err)
	}
}

// TestOpenFDSurvivesConnectionDrop is the marquee T4 test. It seeds an 8 MiB
// file server-side, opens it through the FUSE mount, reads the first 1 MiB
// (establishing a server-side fd), then severs the TCP connection via the
// proxy. After 1 s (well within the 30 s GracePeriod) the proxy is restored.
// The test waits until the client has recovered (a Version RPC round-trips),
// then asserts:
//
//  1. sidAfter == sidBefore: no fresh Create occurred (session was not
//     destroyed and rebuilt — but note this only proves no id change, not that
//     Resume specifically ran vs. the recovery loop simply not having needed a
//     new Create yet).
//
//  2. The same open *os.File reads the remaining 7 MiB to EOF without error
//     and its SHA-256 matches the seeded content.
//
//  3. Server-side Read RPC count increased after the restore: the tail read
//     actually traversed the reconnected session's fd, not just the kernel
//     page cache. This is the proof that the server-side fd survived.
//
// A failed/ESTALE read after restore is a real bug, not an accepted outcome;
// the test reports precise error + session ids for diagnosis.
func (s *ReconnectOpenFDSuite) TestOpenFDSurvivesConnectionDrop() {
	s.Require().NotNil(s.ctx.Proxy(),
		"proxy must be non-nil when WithProxy is used")

	// --- Step 1: seed the file server-side ---
	srcPath := filepath.Join(s.volume.GetSrcPath(), "reconnect-fd.bin")
	wantSHA, err := seedFile(srcPath, seedSize)
	s.Require().NoError(err, "seed file")
	defer os.Remove(srcPath) //nolint:errcheck

	mountPath := filepath.Join(s.volume.GetMountPath(), "reconnect-fd.bin")

	// --- Step 2: open through the mount and read the first 1 MiB ---
	// The OS open causes a server-side Open+Read; this ensures the session
	// has an active fd table entry before the drop.
	f, err := os.Open(mountPath)
	s.Require().NoError(err, "open file through mount")
	defer f.Close() //nolint:errcheck

	chunk0 := make([]byte, 1*1024*1024) // 1 MiB
	_, err = io.ReadFull(f, chunk0)
	s.Require().NoError(err, "read first 1 MiB chunk through mount")

	// Snapshot the Read RPC count after the first chunk. Any increase past
	// this point must come from post-restore server-side Reads.
	readsBefore := s.readRPCs.Load()

	// --- Step 3: capture session id before the drop ---
	sidBefore := s.ctx.GetClient().SessionID()
	s.Require().NotEmpty(sidBefore, "session id must be non-empty before drop")

	// --- Step 4: sever the connection (server stays up) ---
	s.ctx.Proxy().Sever()
	// Sleep 1 s — well under the 30 s GracePeriod — so the reap timer
	// never fires before Resume arrives.
	time.Sleep(1 * time.Second)
	s.ctx.Proxy().Restore()

	// --- Step 5: wait for client recovery ---
	// Poll Version.Get through the same gRPC client until it succeeds; this
	// proves the client's gRPC transport (and TLS handshake) is back up.
	s.Require().Eventually(func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, verr := s.ctx.GetClient().Version().Get(ctx, &proto.VersionRequest{})
		if verr != nil {
			s.T().Logf("recovery probe: %v", verr)
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond,
		"client did not recover within 10 s after proxy restore")

	// Capture session id after recovery.
	sidAfter := s.ctx.GetClient().SessionID()
	s.Require().Equal(sidBefore, sidAfter,
		"session id must be unchanged after reconnect: Resume must reclaim, not Create a new session")

	// --- Step 6: continue reading the SAME open fd to EOF ---
	// Read in a goroutine with a bounded deadline so a hung fd produces a
	// clear failure rather than a test hang.
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
			// A read error here violates the strong contract. Log precise info
			// so the caller can distinguish ESTALE/EBADF (fd invalid → session
			// not reclaimed) from transport errors (retry-path gap).
			s.Failf("open fd read failed after restore (strong contract violated)",
				"error: %v  sidBefore=%s sidAfter=%s",
				res.err, sidBefore, sidAfter)
			return
		}
		rest = res.data
	case <-time.After(20 * time.Second):
		s.FailNow("read-to-EOF timed out after 20 s — fd appears hung after proxy restore")
		return
	}

	// --- Step 7: assert the tail actually hit the server, not just page cache ---
	readsAfter := s.readRPCs.Load()
	s.Require().Greater(readsAfter, readsBefore,
		"server-side Read RPCs must have increased after restore: "+
			"tail must traverse the reconnected fd, not arrive from kernel page cache")

	// --- Verify content integrity ---
	full := append(chunk0, rest...)
	s.Require().Equal(seedSize, len(full),
		"total bytes read must equal the seeded file size")
	gotSHA := sha256.Sum256(full)
	s.Require().True(bytes.Equal(wantSHA, gotSHA[:]),
		"SHA-256 of bytes read across the connection drop must match the seeded content")
	s.T().Logf("open fd read to EOF: %d bytes, sha256 OK, session stable (%s), "+
		"server Read RPCs %d → %d (tail hit server)", len(full), sidAfter, readsBefore, readsAfter)
}

func TestReconnectOpenFDSuite(t *testing.T) {
	suite.Run(t, new(ReconnectOpenFDSuite))
}
