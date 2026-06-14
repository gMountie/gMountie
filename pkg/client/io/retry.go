package io

import (
	"context"
	"time"

	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/metrics"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Retry backoff bounds for the windowed retry core. We keep these in code
// (not config) until evidence shows we need to tune them; the wall-clock
// budget is the user-facing knob (rpc.retry_window).
const (
	retryInitialDelay = 100 * time.Millisecond
	retryMaxDelay     = 1 * time.Second
)

// isRetryableGrpcError reports whether the given error came back from a gRPC
// call with a status code that indicates a transient failure safe to retry
// for an idempotent operation.
func isRetryableGrpcError(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}

// opClass selects the session-change retry policy for an op.
type opClass int

const (
	classIdempotentRead opClass = iota // GetAttr/Access/*XAttr get/ReadDir/Readlink/StatFs — safe across a new session
	classFdOp                          // Read/Write/Flush/Fsync/Release/Allocate/locks — fd dies on a new session
	classPathMutation                  // Mkdir/Rmdir/Rename/Symlink/Link/Unlink/SetAttr/SetXAttr — replay-unsafe on a new session
)

// retryClient is the slice of the gRPC Client that retryOp depends on.
type retryClient interface {
	SessionID() string
	RetryWindow() time.Duration
	Lifetime() context.Context
}

// The real client satisfies retryClient (Tasks 5-7 pass it to retryOp directly).
var _ retryClient = (grpcclient.Client)(nil)

// retryOp runs fn under the transient-retry window. fuseCtx supplies caller
// values; each attempt gets its own perAttempt deadline derived from a
// background base and cancelled when the client lifetime ends (unmount/Close),
// so a spurious FUSE_INTERRUPT can't abort the RPC but unmount can. Transient
// errors (Unavailable/DeadlineExceeded) retry with backoff until the window
// elapses; permanent errors return immediately. On a session-id change
// (Create-fallback recovery), only classIdempotentRead keeps retrying — fd-ops
// and path-mutations stop because their fd is dead / the new idempotency cache
// is empty.
func retryOp[T any](c retryClient, fuseCtx context.Context, op string, class opClass, perAttempt time.Duration, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	window := c.RetryWindow()
	life := c.Lifetime()
	startID := c.SessionID()
	deadline := time.Now().Add(window)
	backoff := retryInitialDelay

	for {
		// Per-attempt ctx: carries fuseCtx values, own deadline, NOT cancelled
		// by the FUSE op ctx (async-preemption fix preserved), but cancelled by
		// the client lifetime.
		attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(fuseCtx), perAttempt)
		stop := context.AfterFunc(life, cancel)
		res, err := fn(attemptCtx)
		stop()
		cancel()

		if err == nil {
			return res, nil
		}
		if !isRetryableGrpcError(err) {
			return zero, err // permanent
		}
		if window <= 0 || life.Err() != nil {
			return zero, err // fail-fast mode, or unmounting
		}
		// Session changed (Create-fallback after a server restart). classFdOp
		// closures run reclaimIfStale on each attempt and reopen their fd, so
		// they keep retrying within the window to self-heal. classPathMutation
		// still bails: it has no fd to reopen and replaying against the new
		// session's empty idempotency cache could surface a spurious
		// EEXIST/ENOENT. classIdempotentRead is path-based and also continues.
		if c.SessionID() != startID && class == classPathMutation {
			return zero, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return zero, err // window exhausted
		}
		sleep := backoff
		if sleep > remaining {
			sleep = remaining
		}
		select {
		case <-time.After(sleep):
		case <-life.Done():
			return zero, err
		}
		if backoff < retryMaxDelay {
			backoff *= 2
			if backoff > retryMaxDelay {
				backoff = retryMaxDelay
			}
		}
		metrics.OnRetry(op, status.Code(err).String())
	}
}
