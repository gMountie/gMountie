package api

// ResilienceSuite is the integration-level proof for the windowed-retry +
// session-change-guard feature (the "resilient mount retry" change). It stands
// up a real server + in-process client over a controllable TCP proxy (so a
// drop can be simulated without killing the server) and drives ops through the
// public BackendClient — the same path the FUSE mount uses — exercising:
//
//   - Resume within grace: a drop shorter than the server session grace period
//     lets the client Resume the SAME session, so an idempotent op that
//     straddles the drop ultimately completes and the session id is unchanged.
//
//   - Clean-fail / no-double-apply past grace: a drop longer than the grace
//     period reaps the server session, so the client falls back to Create (new
//     session id, empty idempotency cache). In that state (1) an fd-op on an fd
//     opened before the drop fails cleanly (no hang), and (2) a path-mutation
//     (Mkdir) whose first attempt applied server-side but lost its reply is NOT
//     replayed on the new session — the directory exists exactly once and the
//     caller does NOT get a spurious EEXIST masking the lost success.
//
// The outage is simulated with utils.TCPProxy.Sever()/Restore() (the server
// stays up the whole time). The lost-reply for the no-double-apply case is
// injected with a client unary interceptor that invokes the real Mkdir (so the
// server applies it) and then returns codes.Unavailable, after forcing the
// session to change — making the guard's decision deterministic rather than
// racing the keepalive recovery loop.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	clientio "go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// resilienceGrace is the short server session grace used by the
	// past-grace scenarios so a sever reaps the session in well under a
	// second. Tests poll for the resulting session-id change rather than
	// sleeping an exact duration, so the precise value is not load-bearing.
	resilienceGrace = 200 * time.Millisecond
	// sessionChangeTimeout bounds how long we wait for the client's recovery
	// loop to Create a fresh session after the server reaps the old one.
	sessionChangeTimeout = 10 * time.Second
)

// mkdirInterceptor is a client unary interceptor that injects a single
// "applied server-side then lost its reply" failure for the first Mkdir RPC.
// On that first Mkdir it invokes the real call (the server applies the dir),
// runs the onApplied hook (which forces the client session to change), and
// then returns codes.Unavailable so the client-side retryOp sees a transient
// error with a now-changed session id — exactly the situation the
// no-double-apply guard must refuse to replay.
type mkdirInterceptor struct {
	tripped   atomic.Bool
	onApplied func() // forces the session change; runs after the server applied attempt 1
}

func (m *mkdirInterceptor) intercept(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	if !strings.HasSuffix(method, "/Mkdir") || !m.tripped.CompareAndSwap(false, true) {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	// Let the server actually apply the Mkdir.
	if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
		return err
	}
	// Force the session to change before we hand the (transient) error back
	// to retryOp, so its post-error session-id check fires deterministically.
	if m.onApplied != nil {
		m.onApplied()
	}
	// Simulate the lost reply: a transient error after the mutation landed.
	return status.Error(codes.Unavailable, "resilience-test: dropped Mkdir reply")
}

type ResilienceSuite struct {
	suite.Suite
}

// severUntilSessionChanges severs the proxy and waits until the server reaps
// the current session (grace elapses) and the client's recovery loop creates a
// fresh one, then restores the proxy. Returns the old and new session ids.
func (s *ResilienceSuite) severUntilSessionChanges(appCtx *utils.AppTestingContext) (string, string) {
	s.T().Helper()
	oldID := appCtx.GetClient().SessionID()
	s.Require().NotEmpty(oldID, "session must be established before severing")

	appCtx.Proxy().Sever()
	// Restore once the grace has comfortably elapsed so the client's
	// reconnect dials succeed and Resume gets a definitive "not found" →
	// Create. The reap fires after `resilienceGrace`; give it margin.
	go func() {
		time.Sleep(3 * resilienceGrace)
		appCtx.Proxy().Restore()
	}()

	var newID string
	s.Require().Eventually(func() bool {
		newID = appCtx.GetClient().SessionID()
		return newID != "" && newID != oldID
	}, sessionChangeTimeout, 10*time.Millisecond,
		"client must Create a fresh session after the server reaps the old one")
	return oldID, newID
}

// TestResumesWithinGrace: a drop shorter than the grace period is reclaimed by
// Resume, so an idempotent op straddling the drop completes and the session id
// is unchanged.
func (s *ResilienceSuite) TestResumesWithinGrace() {
	appCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithProxy(),
		// Long grace: any brief sever is well within it, so Resume holds.
		utils.WithSessionGracePeriod(5*time.Second),
	)
	s.Require().NoError(err)
	s.Require().NoError(appCtx.Start())
	defer func() { s.Require().NoError(appCtx.Close()) }()

	backend := clientio.NewBackendClient(appCtx.GetClient(), appCtx.GetVolumes()[0].Name)
	startID := appCtx.GetClient().SessionID()
	s.Require().NotEmpty(startID)

	// Sever briefly, then restore — shorter than the grace.
	appCtx.Proxy().Sever()
	go func() {
		time.Sleep(300 * time.Millisecond)
		appCtx.Proxy().Restore()
	}()

	// A GetAttr issued across the drop must ultimately return OK: retryOp
	// keeps retrying the idempotent read (WaitForReady) until the conn is
	// restored and the session Resumes.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, st := backend.Stat(ctx, "/")
	s.Require().Equal(fuse.OK, st, "GetAttr straddling a sub-grace drop must succeed")

	// Session id unchanged: the drop was reclaimed by Resume, not Create.
	s.Require().Equal(startID, appCtx.GetClient().SessionID(),
		"a sub-grace drop must Resume the same session (no Create)")
}

// TestFdOpFailsCleanlyPastGrace: after the session is reaped (drop > grace) the
// client Creates a new session; an fd-op on an fd opened before the drop must
// fail cleanly (a bounded error, not a hang) because that fd is dead on the new
// session.
func (s *ResilienceSuite) TestFdOpFailsCleanlyPastGrace() {
	srcDir := s.T().TempDir()
	// Seed a file so Open + Read have something to target.
	s.Require().NoError(os.WriteFile(filepath.Join(srcDir, "data.bin"), []byte("hello-resilience"), 0o644))

	appCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume("vol", srcDir),
		utils.WithProxy(),
		utils.WithSessionGracePeriod(resilienceGrace),
	)
	s.Require().NoError(err)
	s.Require().NoError(appCtx.Start())
	defer func() { s.Require().NoError(appCtx.Close()) }()

	backend := clientio.NewBackendClient(appCtx.GetClient(), "vol")

	// Open an fd on the pre-drop session.
	fh, st := backend.Open(context.Background(), "/data.bin", uint32(os.O_RDONLY))
	s.Require().Equal(fuse.OK, st, "Open before the drop must succeed")

	// Drop long enough to reap the session → client Creates a fresh one.
	oldID, newID := s.severUntilSessionChanges(appCtx)
	s.Require().NotEqual(oldID, newID)

	// A Read on the old fd must fail cleanly and promptly — the fd is dead on
	// the new session, so the server returns NotFound ("session not found").
	// That is a permanent (non-retryable) gRPC code, so retryOp short-circuits
	// immediately and the caller gets a bounded error rather than a hang. (The
	// classFdOp session-change guard, which stops a *retryable* fd-op error
	// after a session change, is covered by the retryOp unit tests.)
	done := make(chan fuse.Status, 1)
	go func() {
		dest := make([]byte, 16)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, rst := backend.Read(ctx, fh, 0, dest)
		done <- rst
	}()
	select {
	case rst := <-done:
		s.Require().False(rst.Ok(), "fd-op on a reaped session must fail, got OK (silent data loss)")
	case <-time.After(20 * time.Second):
		s.FailNow("fd-op on a reaped session hung instead of failing cleanly")
	}
}

// TestNoDoubleApplyPastGrace: a Mkdir whose first attempt applied server-side
// but lost its reply across a session reap is NOT retried on the new session.
// The directory exists exactly once, and the caller gets EIO (honest "unknown")
// rather than a spurious EEXIST that would mask the lost success.
func (s *ResilienceSuite) TestNoDoubleApplyPastGrace() {
	srcDir := s.T().TempDir()

	ic := &mkdirInterceptor{}
	var appCtx *utils.AppTestingContext
	// onApplied forces the session to change after the server applied attempt 1
	// and blocks until it has, so retryOp's post-error session check is
	// deterministic. severUntilSessionChanges only returns once the id flipped.
	var severOnce sync.Once
	ic.onApplied = func() {
		severOnce.Do(func() { s.severUntilSessionChanges(appCtx) })
	}

	var err error
	appCtx, err = utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume("vol", srcDir),
		utils.WithProxy(),
		utils.WithSessionGracePeriod(resilienceGrace),
		utils.WithClientUnaryInterceptors(ic.intercept),
	)
	s.Require().NoError(err)
	s.Require().NoError(appCtx.Start())
	defer func() { s.Require().NoError(appCtx.Close()) }()

	backend := clientio.NewBackendClient(appCtx.GetClient(), "vol")
	oldID := appCtx.GetClient().SessionID()
	s.Require().NotEmpty(oldID)

	// Drive the Mkdir: attempt 1 applies server-side, the interceptor forces a
	// session change and returns Unavailable; retryOp must then refuse to
	// replay (classPathMutation across a session change) and surface EIO.
	_, st := backend.Mkdir(context.Background(), "/newdir", 0o755)

	s.Require().True(ic.tripped.Load(), "the lost-reply interceptor must have fired on the Mkdir")
	s.Require().NotEqual(oldID, appCtx.GetClient().SessionID(),
		"the session must have changed (Create after reap) for this to test the guard")

	// The caller gets EIO — honest "I don't know if it applied" — NOT a
	// spurious EEXIST that would falsely report the dir as pre-existing.
	s.Require().Equal(fuse.EIO, st,
		"a path-mutation across a session change must surface EIO, not a replayed status")
	s.Require().NotEqual(fuse.Status(syscall.EEXIST), st,
		"the caller must NOT receive a spurious EEXIST masking the lost success")

	// The directory exists exactly once on disk (applied by attempt 1, never
	// replayed). A double-apply would have surfaced EEXIST above.
	info, statErr := os.Stat(filepath.Join(srcDir, "newdir"))
	s.Require().NoError(statErr, "attempt 1 must have created the directory exactly once")
	s.Require().True(info.IsDir())

	// Prove what the guard avoided: on the NEW session (empty idempotency
	// cache) a replay of the same Mkdir WOULD surface EEXIST. That the caller
	// got EIO above — not this EEXIST — is exactly the guard working.
	_, replaySt := backend.Mkdir(context.Background(), "/newdir", 0o755)
	s.Require().Equal(fuse.Status(syscall.EEXIST), replaySt,
		"sanity: a fresh Mkdir of the existing dir on the new session surfaces EEXIST; "+
			"the guard prevented exactly this spurious status from reaching the first caller")
}

// TestFdOpReclaimsAcrossRestart proves the complement of TestFdOpFailsCleanlyPastGrace:
// when the server RESTARTS (new boot epoch + empty session table), reclaimIfStale
// detects the epoch change and reopens the fd by path, so a Read on the
// pre-restart handle SUCCEEDS and returns the original content.
//
// This test uses WithTCPTransport (no proxy) because RestartServerNewSession
// rebinds the TCP listener; bufconn cannot rebind.
func (s *ResilienceSuite) TestFdOpReclaimsAcrossRestart() {
	srcDir := s.T().TempDir()
	const content = "hello-reclaim"
	s.Require().NoError(os.WriteFile(filepath.Join(srcDir, "data.bin"), []byte(content), 0o644))

	appCtx, err := utils.NewAppTestingContext(
		utils.WithTCPTransport(),
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume("vol", srcDir),
		utils.WithSessionGracePeriod(resilienceGrace),
	)
	s.Require().NoError(err)
	s.Require().NoError(appCtx.Start())
	defer func() { s.Require().NoError(appCtx.Close()) }()

	backend := clientio.NewBackendClient(appCtx.GetClient(), "vol")

	// Open an fd on the pre-restart session.
	fh, st := backend.Open(context.Background(), "/data.bin", uint32(os.O_RDONLY))
	s.Require().Equal(fuse.OK, st, "Open before restart must succeed")

	oldID := appCtx.GetClient().SessionID()
	s.Require().NotEmpty(oldID)

	// Restart with a fresh epoch + empty session table.
	s.Require().NoError(appCtx.RestartServerNewSession())

	// Wait for the client's keepalive loop to Create a new session (Resume will
	// fail against the empty table, causing a Create with a new session id).
	s.Require().Eventually(func() bool {
		id := appCtx.GetClient().SessionID()
		return id != "" && id != oldID
	}, 10*time.Second, 50*time.Millisecond,
		"client must Create a new session after restart")

	// Read on the SAME fd in a bounded goroutine — reclaimIfStale must detect
	// the epoch change and reopen by path, so the Read succeeds.
	type result struct {
		n  int
		st fuse.Status
	}
	done := make(chan result, 1)
	go func() {
		dest := make([]byte, len(content))
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		n, rst := backend.Read(ctx, fh, 0, dest)
		done <- result{n, rst}
	}()

	select {
	case r := <-done:
		s.Require().Equal(fuse.OK, r.st, "fd-op must reclaim and succeed across a true restart")
		dest := make([]byte, len(content))
		// Re-read to get the actual bytes (done channel carries n, not dest).
		// Redo the read synchronously now that the handle is reclaimed.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		n, rst2 := backend.Read(ctx, fh, 0, dest)
		s.Require().Equal(fuse.OK, rst2, "second read after reclaim must succeed")
		s.Require().Equal(content, string(dest[:n]), "reclaimed fd must return the original content")
	case <-time.After(20 * time.Second):
		s.FailNow("fd-op after a true restart hung instead of reclaiming")
	}
}

func TestResilienceSuite(t *testing.T) {
	suite.Run(t, new(ResilienceSuite))
}
