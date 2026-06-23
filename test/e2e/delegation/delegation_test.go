// Package delegation provides end-to-end coherence tests for write-delegation.
// Two independent gRPC clients share one in-process server over bufconn (no FUSE).
// Each client's delegation stack is assembled manually:
//   - delegation.Manager (client-side grant oracle + recall handler)
//   - transport.NewBackendClient (gRPC leaf) wired with WithDelegationHook
//   - delegation.NewLayer (write-path recorder)
//   - inline recall pump goroutine (mirrors mount.runRecallLoop)
//
// The four scenarios cover: grant+coherence, recall round-trip, cross-subtree
// rename, and thrash-to-cooldown.
package delegation

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	clientdelegation "go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/client/backend/transport"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/test/e2e/utils"
)

// ─── test-double ────────────────────────────────────────────────────────────

// trackingInvalidator records calls to InvalidateSubtree. It implements
// delegation.CacheInvalidator so delegation.Manager can notify it on recall.
type trackingInvalidator struct {
	mu    sync.Mutex
	roots []string
}

func (t *trackingInvalidator) InvalidateSubtree(root string) {
	t.mu.Lock()
	t.roots = append(t.roots, root)
	t.mu.Unlock()
}

func (t *trackingInvalidator) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.roots)
}

// ─── client stack ───────────────────────────────────────────────────────────

// clientStack is a fully-wired delegation client without FUSE.
type clientStack struct {
	cl           grpcclient.Client
	mgr          *clientdelegation.Manager
	inv          *trackingInvalidator
	be           backend.FileSystemBackend
	cancelRecall context.CancelFunc
	// mkdirSeq is used to generate unique sub-directory names on retry so
	// EEXIST doesn't block grant delivery (applyGrant only fires on FS_OK).
	mkdirSeq atomic.Uint64
}

// MkdirFresh creates a uniquely named child under parent, returning the granted
// root once the Manager holds a grant covering parent. Each attempt uses a new
// name so the server always sees a genuine create (no EEXIST → no skipped grant).
// The created directories accumulate under parent; callers should clean them up.
func (cs *clientStack) MkdirFresh(ctx context.Context, parent string) (proto.FsError, string) {
	name := fmt.Sprintf("sub%d", cs.mkdirSeq.Add(1))
	path := parent + "/" + name
	_, st := cs.be.Mkdir(ctx, path, 0o755)
	return st, path
}

// Rename issues a Rename via the layered backend.
func (cs *clientStack) Rename(ctx context.Context, old, newp string) proto.FsError {
	return cs.be.Rename(ctx, old, newp)
}

// Close tears down the stack.
func (cs *clientStack) Close() {
	cs.cancelRecall()
	cs.mgr.Close()
	_ = cs.be.Close()
	_ = cs.cl.Close()
}

// ─── helpers ────────────────────────────────────────────────────────────────

// newStack creates one client stack for volume volName connected to tc's server.
func newStack(t *testing.T, tc *utils.AppTestingContext, user, pass, volName string) *clientStack {
	t.Helper()
	cl, err := tc.NewClientAs(user, pass)
	require.NoError(t, err, "NewClientAs")

	inv := &trackingInvalidator{}
	mgr := clientdelegation.NewManager(inv)

	rawBe := transport.NewBackendClient(cl, volName, transport.WithDelegationHook(mgr))
	layerBe := clientdelegation.NewLayer(rawBe, mgr)

	// Wire the recall goroutine context into the manager.
	recallCtx, cancelRecall := context.WithCancel(context.Background())
	mgr.SetCancel(cancelRecall)

	cs := &clientStack{cl: cl, mgr: mgr, inv: inv, be: layerBe, cancelRecall: cancelRecall}

	// Start the inline recall pump.
	go runRecallPump(recallCtx, cl, mgr)

	return cs
}

// runRecallPump mirrors mount.runRecallLoop: opens the bidi Recall stream,
// delivers OnRecall to the manager for each message, and sends the ack.
// Retries with backoff when the stream drops.
func runRecallPump(ctx context.Context, cl grpcclient.Client, mgr *clientdelegation.Manager) {
	backoff := 50 * time.Millisecond
	const maxBackoff = 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		stream, err := cl.Fs().Recall(ctx, grpc.WaitForReady(true))
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}
		backoff = 50 * time.Millisecond // reset on successful open
		for {
			msg, err := stream.Recv()
			if err != nil {
				break // stream dropped; outer loop reconnects
			}
			mgr.OnRecall(msg.Root)
			_ = stream.Send(&proto.RecallAck{RecallId: msg.RecallId, Done: true})
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// bg returns a plain background context for direct backend calls.
func bg() context.Context { return context.Background() }

// ─── suite ──────────────────────────────────────────────────────────────────

// DelegationCoherenceSuite exercises the full delegation stack (arbiter +
// recall + transport hook + manager) over bufconn with two independent clients
// sharing one in-process server. No FUSE mount is required.
type DelegationCoherenceSuite struct {
	suite.Suite
	tc      *utils.AppTestingContext
	srcDir  string
	volName string
}

func (s *DelegationCoherenceSuite) SetupSuite() {
	s.srcDir = s.T().TempDir()
	s.volName = "testvol"

	tc, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("user", "pass"),
		utils.WithExistingVolume(s.volName, s.srcDir),
		// The RecallRegistry timeout is keyed off the session grace period
		// (NewRecallRegistry(cfg.Server.Session.GracePeriod)). The test harness
		// defaults GracePeriod to zero, which makes all recall RTTs time out
		// immediately. Set a short but non-zero value so recalls can complete.
		utils.WithSessionGracePeriod(5*time.Second),
	)
	s.Require().NoError(err, "build AppTestingContext")
	s.Require().NoError(tc.Start(), "start server")
	s.tc = tc
}

func (s *DelegationCoherenceSuite) TearDownSuite() {
	if s.tc != nil {
		_ = s.tc.Close()
	}
}

// acquireGrant seeds the manager's write-set with two entries under parent,
// then issues Mkdir calls until the Manager holds a grant covering parent.
// It relies on MkdirFresh to use unique names so EEXIST never blocks the grant.
// The backing directory must already exist under srcDir.
func (s *DelegationCoherenceSuite) acquireGrant(cs *clientStack, parent string, deadline time.Duration) {
	s.T().Helper()
	// Seed the write-set so LCA resolves to parent.
	cs.mgr.Record(parent + "/seed1")
	cs.mgr.Record(parent + "/seed2")

	s.Require().Eventually(func() bool {
		st, _ := cs.MkdirFresh(bg(), parent)
		// EAGAIN means the write-set is still empty or the server returned a
		// transient error; continue. Only FS_OK delivers the grant.
		return st == proto.FsError_FS_OK && cs.mgr.IsDelegated(parent)
	}, deadline, 20*time.Millisecond, "must obtain delegation grant for %q", parent)
}

// ─── Scenario 1: GRANT + COHERENCE ──────────────────────────────────────────
// Client A acquires delegation for "d". B mutates d/x → server recalls A.
// Assert: B's Mkdir succeeds AND A.IsDelegated("d") becomes false.

func (s *DelegationCoherenceSuite) TestGrantAndCoherence() {
	r := s.Require()

	// Create backing dir "d" on the server's source tree.
	r.NoError(os.Mkdir(s.srcDir+"/d", 0o755))
	defer os.RemoveAll(s.srcDir + "/d")

	csA := newStack(s.T(), s.tc, "user", "pass", s.volName)
	defer csA.Close()
	csB := newStack(s.T(), s.tc, "user", "pass", s.volName)
	defer csB.Close()

	// A obtains delegation for "d".
	s.acquireGrant(csA, "d", 5*time.Second)
	r.True(csA.mgr.IsDelegated("d"), "A must hold grant for 'd' before B contends")

	// B contends: Mkdir d/x. Server arbitrates → recalls A → B's op proceeds.
	// On the first attempt, A's recall stream may not yet be registered; retry.
	r.Eventually(func() bool {
		st, _ := csB.MkdirFresh(bg(), "d")
		return st == proto.FsError_FS_OK
	}, 5*time.Second, 50*time.Millisecond, "B.Mkdir must succeed after A is recalled")

	// A's grant must be dropped by the recall.
	r.Eventually(func() bool {
		return !csA.mgr.IsDelegated("d")
	}, 5*time.Second, 50*time.Millisecond, "A must lose delegation for 'd' after B contends")
}

// ─── Scenario 2: RECALL ROUND-TRIP ──────────────────────────────────────────
// Stronger assertion: the full recall RTT must be confirmed by A's
// trackingInvalidator — proving OnRecall fired (not just B succeeded).

func (s *DelegationCoherenceSuite) TestRecallRoundTrip() {
	r := s.Require()

	r.NoError(os.Mkdir(s.srcDir+"/recall-root", 0o755))
	defer os.RemoveAll(s.srcDir + "/recall-root")

	csA := newStack(s.T(), s.tc, "user", "pass", s.volName)
	defer csA.Close()
	csB := newStack(s.T(), s.tc, "user", "pass", s.volName)
	defer csB.Close()

	// A acquires delegation for "recall-root".
	s.acquireGrant(csA, "recall-root", 5*time.Second)

	invBefore := csA.inv.count()

	// B contends under recall-root → recall must complete before B returns OK.
	r.Eventually(func() bool {
		st, _ := csB.MkdirFresh(bg(), "recall-root")
		return st == proto.FsError_FS_OK
	}, 5*time.Second, 50*time.Millisecond, "B must succeed after full recall round-trip")

	// A's grant is dropped.
	r.Eventually(func() bool {
		return !csA.mgr.IsDelegated("recall-root")
	}, 5*time.Second, 50*time.Millisecond, "A must lose delegation after recall")

	// trackingInvalidator was called by OnRecall → the whole stream path fired.
	r.Eventually(func() bool {
		return csA.inv.count() > invBefore
	}, 5*time.Second, 50*time.Millisecond, "A's invalidator must be called via OnRecall (stream path confirmed)")
}

// ─── Scenario 3: CROSS-SUBTREE RENAME ───────────────────────────────────────
// A holds "sub-a". B issues rename sub-a/x → sub-b/x (cross-subtree).
// Server arbitrates oldPath (recall A) and newPath (free).
// Assert: no panic, rename returns OK or ENOENT (src may vanish), A loses sub-a.

func (s *DelegationCoherenceSuite) TestCrossSubtreeRename() {
	r := s.Require()

	r.NoError(os.MkdirAll(s.srcDir+"/sub-a/x", 0o755))
	r.NoError(os.MkdirAll(s.srcDir+"/sub-b", 0o755))
	defer os.RemoveAll(s.srcDir + "/sub-a")
	defer os.RemoveAll(s.srcDir + "/sub-b")

	csA := newStack(s.T(), s.tc, "user", "pass", s.volName)
	defer csA.Close()
	csB := newStack(s.T(), s.tc, "user", "pass", s.volName)
	defer csB.Close()

	// A acquires delegation over "sub-a". sub-a/x already exists; seed fresh names.
	csA.mgr.Record("sub-a/x")
	csA.mgr.Record("sub-a/y")
	// Create sub-a/y on disk so Mkdir can succeed (needed to get grant).
	r.Eventually(func() bool {
		st, _ := csA.MkdirFresh(bg(), "sub-a")
		return st == proto.FsError_FS_OK && csA.mgr.IsDelegated("sub-a")
	}, 5*time.Second, 20*time.Millisecond, "A must hold grant for sub-a")

	// B renames cross-subtree: sub-a/x → sub-b/x.
	// Server recalls A for sub-a (foreign delegation), sub-b is free.
	// No panic + terminal status (OK or ENOENT) is the contract.
	r.Eventually(func() bool {
		st := csB.Rename(bg(), "sub-a/x", "sub-b/x")
		return st == proto.FsError_FS_OK || st == proto.FsError_FS_ENOENT
	}, 5*time.Second, 50*time.Millisecond, "B cross-subtree rename must complete without panic")

	// A must have lost sub-a grant (recalled during rename arbitration).
	r.Eventually(func() bool {
		return !csA.mgr.IsDelegated("sub-a")
	}, 5*time.Second, 50*time.Millisecond, "A must lose sub-a grant after cross-subtree rename")
}

// ─── Scenario 4: THRASH → COOLDOWN ──────────────────────────────────────────
// After a recall the root enters cooldown. The arbiter denies re-grant:
// Request returns GrantedRoot=="" with RetryAfterMs>0. We access the arbiter
// via GetServerApp().Delegation.
//
// Note: DelegationCooldownTrips is defined in pkg/server/metrics but not yet
// wired to the arbiter's cooldown.trip() call, so the Prometheus counter
// cannot be used here. The arbiter's Request() return value is the direct signal.

func (s *DelegationCoherenceSuite) TestThrashCooldown() {
	r := s.Require()

	r.NoError(os.Mkdir(s.srcDir+"/thrash", 0o755))
	defer os.RemoveAll(s.srcDir + "/thrash")

	csA := newStack(s.T(), s.tc, "user", "pass", s.volName)
	defer csA.Close()
	csB := newStack(s.T(), s.tc, "user", "pass", s.volName)
	defer csB.Close()

	arb := s.tc.GetServerApp().Delegation
	r.NotNil(arb, "delegation arbiter must be wired on server AppContext")

	// A acquires delegation over "thrash".
	s.acquireGrant(csA, "thrash", 5*time.Second)
	r.True(csA.mgr.IsDelegated("thrash"), "A must hold grant for thrash")

	// B contends → server recalls A → cooldown trips for "thrash".
	r.Eventually(func() bool {
		st, _ := csB.MkdirFresh(bg(), "thrash")
		return st == proto.FsError_FS_OK
	}, 5*time.Second, 50*time.Millisecond, "B must succeed triggering recall+cooldown")

	// A loses the grant.
	r.Eventually(func() bool {
		return !csA.mgr.IsDelegated("thrash")
	}, 5*time.Second, 50*time.Millisecond, "A must lose thrash grant after recall")

	// The arbiter's cooldown denies re-grant for "thrash": empty root + retry hint.
	sessionA := csA.cl.SessionID()
	r.Eventually(func() bool {
		g := arb.Request(sessionA, "thrash")
		return g.GetGrantedRoot() == "" && g.GetRetryAfterMs() > 0
	}, 2*time.Second, 10*time.Millisecond,
		"arbiter must deny re-grant for 'thrash' while in cooldown (empty GrantedRoot, RetryAfterMs>0)")
}

// ─── entry point ────────────────────────────────────────────────────────────

func TestDelegationCoherenceSuite(t *testing.T) {
	suite.Run(t, new(DelegationCoherenceSuite))
}
