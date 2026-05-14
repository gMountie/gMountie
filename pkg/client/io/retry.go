package io

import (
	"context"
	"time"

	"github.com/avast/retry-go/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryAttempts is the hardcoded number of attempts for idempotent RPCs.
// We keep this in code (not config) until evidence shows we need to tune it.
const (
	retryAttempts     = 3
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

// retryableCall invokes fn up to retryAttempts times with exponential
// backoff, retrying only when isRetryableGrpcError says so. The returned
// value/error is the result of the final attempt. The function is generic
// over the RPC reply type T so call sites read naturally.
//
// retryableCall is safe for both idempotent and idempotency-token-stamped
// mutating ops. Idempotent ops (GetAttr, OpenDir, Access, GetXAttr,
// StatFs, Read) can call it directly. Mutating ops MUST allocate a
// request_id (uuid.NewString()) outside the closure passed to this
// function and stamp it on the request struct, so the server's
// per-session idempotency cache short-circuits any retry that the
// network or a stalled server forced us into.
func retryableCall[T any](ctx context.Context, op string, fn func(context.Context) (T, error)) (T, error) {
	var result T
	err := retry.Do(
		func() error {
			r, err := fn(ctx)
			if err != nil {
				return err
			}
			result = r
			return nil
		},
		retry.RetryIf(isRetryableGrpcError),
		retry.Attempts(retryAttempts),
		retry.Delay(retryInitialDelay),
		retry.MaxDelay(retryMaxDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.Context(ctx),
		retry.LastErrorOnly(true),
	)
	return result, err
}

// withMetaTimeout returns a context bounded by the configured metadata
// timeout. Callers must defer the returned cancel function.
func withMetaTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

// withIOTimeout returns a context bounded by the configured I/O timeout.
// Callers must defer the returned cancel function.
func withIOTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}
