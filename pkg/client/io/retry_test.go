package io

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryableGrpcError_Codes(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRetryableGrpcError(tt.err))
		})
	}
}

func TestRetryableCall_SucceedsFirstTry(t *testing.T) {
	calls := 0
	res, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		return 42, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 42, res)
	assert.Equal(t, 1, calls)
}

func TestRetryableCall_RetriesOnRetryableError(t *testing.T) {
	calls := 0
	res, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, status.Error(codes.Unavailable, "still down")
		}
		return 7, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 7, res)
	assert.Equal(t, 3, calls)
}

func TestRetryableCall_GivesUpAfterMaxAttempts(t *testing.T) {
	calls := 0
	_, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.Unavailable, "still down")
	})
	assert.Error(t, err)
	assert.Equal(t, 3, calls, "should attempt 3 times then stop")
}

func TestRetryableCall_DoesNotRetryNonRetryableError(t *testing.T) {
	calls := 0
	_, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.NotFound, "missing")
	})
	assert.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestRetryableCall_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := retryableCall(ctx, "test", func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.Unavailable, "down")
	})
	assert.Error(t, err)
	// Could be 1 or 2 depending on timing — never the full 3.
	assert.Less(t, calls, 3)
}

func TestWithMetaTimeout_DerivesDeadline(t *testing.T) {
	parent := context.Background()
	ctx, cancel := withMetaTimeout(parent, 100*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(100*time.Millisecond), deadline, 50*time.Millisecond)
}

func TestWithIOTimeout_DerivesDeadline(t *testing.T) {
	parent := context.Background()
	ctx, cancel := withIOTimeout(parent, 100*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(100*time.Millisecond), deadline, 50*time.Millisecond)
}
