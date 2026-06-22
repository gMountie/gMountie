package io

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/metrics"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RetryTestSuite struct {
	suite.Suite
}

func (s *RetryTestSuite) TestIsRetryableGrpcError_Codes() {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"non-grpc plain error", errors.New("boom"), false},
		{"Unavailable", status.Error(codes.Unavailable, "down"), true},
		{"DeadlineExceeded", status.Error(codes.DeadlineExceeded, "slow"), true},
		{"NotFound", status.Error(codes.NotFound, "no"), false},
		{"InvalidArgument", status.Error(codes.InvalidArgument, "bad"), false},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.want, isRetryableGrpcError(tt.err))
		})
	}
}

func TestRetryTestSuite(t *testing.T) {
	suite.Run(t, new(RetryTestSuite))
}

// fakeRetryClient drives the surface retryOp touches — SessionID() and a
// controllable lifetime/window — so the property tests need no real gRPC.
type fakeRetryClient struct {
	mu      sync.Mutex
	id      string
	life    context.Context
	window  time.Duration
	meta    time.Duration
	metrics *metrics.Metrics // nil unless a test wants to observe RetryInc
}

func (f *fakeRetryClient) SessionID() string          { f.mu.Lock(); defer f.mu.Unlock(); return f.id }
func (f *fakeRetryClient) setID(s string)             { f.mu.Lock(); f.id = s; f.mu.Unlock() }
func (f *fakeRetryClient) RetryWindow() time.Duration { return f.window }
func (f *fakeRetryClient) Lifetime() context.Context  { return f.life }
func (f *fakeRetryClient) MetaTimeout() time.Duration { return f.meta }
func (f *fakeRetryClient) Metrics() *metrics.Metrics  { return f.metrics }

type RetryOpSuite struct {
	suite.Suite
}

// Transient-then-OK: succeeds after K failures, multiple attempts in one window.
func (s *RetryOpSuite) TestRetriesTransientUntilSuccess() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 50 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, status.Error(codes.Unavailable, "down")
		}
		return 7, nil
	})
	s.Require().NoError(err)
	s.GreaterOrEqual(calls, 3)
}

// Permanent error returns immediately, no retry.
func (s *RetryOpSuite) TestPermanentNoRetry() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 50 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.PermissionDenied, "no")
	})
	s.Require().Error(err)
	s.Equal(1, calls)
}

// Window expiry: always transient -> returns after window, >1 attempt.
func (s *RetryOpSuite) TestWindowExpiry() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 150 * time.Millisecond, meta: 20 * time.Millisecond}
	calls := 0
	start := time.Now()
	_, err := retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.Unavailable, "down")
	})
	s.Require().Error(err)
	s.Greater(calls, 1)
	s.GreaterOrEqual(time.Since(start), 150*time.Millisecond)
}

// window==0 -> exactly one attempt.
func (s *RetryOpSuite) TestZeroWindowSingleAttempt() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 0, meta: 20 * time.Millisecond}
	calls := 0
	_, _ = retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.Unavailable, "down")
	})
	s.Equal(1, calls)
}

// Session-change guard: a path-mutation stops once SessionID changes.
func (s *RetryOpSuite) TestMutationStopsOnSessionChange() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 20 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "Mkdir", classPathMutation, f.meta, func(ctx context.Context) (int, error) {
		calls++
		f.setID("B") // recovery created a new session mid-op
		return 0, status.Error(codes.Unavailable, "down")
	})
	s.Require().Error(err)
	s.Equal(1, calls) // did not retry across the session change
}

// The FUSE-op ctx cancelling (a spurious FUSE_INTERRUPT from Go's
// async-preemption SIGURG) must NOT abort an in-flight attempt — that is the
// exact bug the context.WithoutCancel(fuseCtx) stance fixes. The client
// lifetime cancelling (unmount/Close), by contrast, MUST abort it via AfterFunc.
func (s *RetryOpSuite) TestFuseCtxCancelDoesNotAbortButLifetimeDoes() {
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()
	// Long per-attempt timeout + window so neither fires during the test;
	// only the two cancellations under test can move the attempt ctx.
	f := &fakeRetryClient{id: "A", life: lifeCtx, window: 10 * time.Second, meta: time.Hour}
	fuseCtx, fuseCancel := context.WithCancel(context.Background())

	entered := make(chan context.Context, 1)
	done := make(chan error, 1)
	go func() {
		_, err := retryOp[int](f, fuseCtx, "Op", classIdempotentRead, f.meta,
			func(ctx context.Context) (int, error) {
				entered <- ctx
				<-ctx.Done() // block until the attempt ctx is cancelled
				return 0, status.FromContextError(ctx.Err()).Err()
			})
		done <- err
	}()

	attemptCtx := <-entered

	// Phase 1: cancelling the FUSE ctx must NOT reach the in-flight attempt.
	fuseCancel()
	select {
	case <-attemptCtx.Done():
		s.Fail("attempt ctx aborted by FUSE-ctx cancellation; WithoutCancel is broken")
	case <-time.After(100 * time.Millisecond):
		// still alive — correct
	}

	// Phase 2: cancelling the client lifetime MUST abort the attempt.
	lifeCancel()
	select {
	case <-attemptCtx.Done():
		// correct
	case <-time.After(2 * time.Second):
		s.Fail("attempt ctx not aborted by lifetime cancellation; AfterFunc is broken")
	}
	s.Require().Error(<-done)
}

// fd-op continues across a session change so reclaimIfStale can self-heal on
// the next attempt. The old guard bailed classFdOp the moment the session id
// flipped — this test pins the new behaviour (keep retrying inside the window).
func (s *RetryOpSuite) TestFdOpContinuesAcrossSessionChange() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 20 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "Read", classFdOp, f.meta, func(ctx context.Context) (int, error) {
		calls++
		f.setID("B") // simulate Create-fallback: new session mid-op
		if calls < 2 {
			return 0, status.Error(codes.Unavailable, "down")
		}
		return 42, nil // reclaimIfStale would have reopened the fd; we just succeed
	})
	s.Require().NoError(err)
	s.Equal(2, calls) // retried across the session change and healed
}

// path-mutation bails immediately when the session id changes: replaying
// against the new session's empty idempotency cache could surface a spurious
// EEXIST/ENOENT, so we stop and return the error.
func (s *RetryOpSuite) TestPathMutationBailsOnSessionChange() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 20 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "Mkdir", classPathMutation, f.meta, func(ctx context.Context) (int, error) {
		calls++
		f.setID("B") // session changed on the first attempt
		return 0, status.Error(codes.Unavailable, "down")
	})
	s.Require().Error(err)
	s.Equal(1, calls) // bailed after the first attempt, did not retry
}

// Idempotent read continues across a session change.
func (s *RetryOpSuite) TestReadContinuesOnSessionChange() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 20 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "GetAttr", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		f.setID("B")
		if calls < 2 {
			return 0, status.Error(codes.Unavailable, "down")
		}
		return 1, nil
	})
	s.Require().NoError(err)
	s.Equal(2, calls)
}

// Protective property (per the project's "test the property the fix
// protects" rule): EVERY retry attempt must observe a fresh, non-expired
// per-attempt deadline — not a single deadline-bound context shared across
// attempts, which would make DeadlineExceeded-retry dead code (the bug class
// the windowed retryOp replaced). Failing attempts return DeadlineExceeded
// specifically, so this also proves that code is genuinely retryable now.
func (s *RetryOpSuite) TestEachAttemptObservesFreshDeadline() {
	const perAttempt = 50 * time.Millisecond
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 10 * time.Second, meta: perAttempt}
	var remaining []time.Duration
	calls := 0
	_, err := retryOp[int](f, context.Background(), "Op", classIdempotentRead, perAttempt,
		func(ctx context.Context) (int, error) {
			calls++
			dl, ok := ctx.Deadline()
			s.Require().True(ok, "attempt ctx must carry a deadline")
			remaining = append(remaining, time.Until(dl))
			if calls < 4 {
				return 0, status.Error(codes.DeadlineExceeded, "attempt timed out")
			}
			return 1, nil
		})
	s.Require().NoError(err)
	s.Require().Len(remaining, 4)
	for i, r := range remaining {
		s.Greaterf(r, perAttempt/2,
			"attempt %d observed %v left of its %v budget — a shared/expired deadline leaked across attempts", i+1, r, perAttempt)
	}
}

// Lifetime cancel aborts promptly.
func (s *RetryOpSuite) TestLifetimeCancelAborts() {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeRetryClient{id: "A", life: ctx, window: 10 * time.Second, meta: 20 * time.Millisecond}
	go func() { time.Sleep(40 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(c context.Context) (int, error) {
		return 0, status.Error(codes.Unavailable, "down")
	})
	s.Require().Error(err)
	s.Less(time.Since(start), 2*time.Second)
}

func TestRetryOpSuite(t *testing.T) {
	suite.Run(t, new(RetryOpSuite))
}
