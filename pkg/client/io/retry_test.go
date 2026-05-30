package io

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
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

func (s *RetryTestSuite) TestRetryableCall_SucceedsFirstTry() {
	calls := 0
	res, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		return 42, nil
	})
	s.Require().NoError(err)
	s.Equal(42, res)
	s.Equal(1, calls)
}

func (s *RetryTestSuite) TestRetryableCall_RetriesOnRetryableError() {
	calls := 0
	res, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, status.Error(codes.Unavailable, "still down")
		}
		return 7, nil
	})
	s.Require().NoError(err)
	s.Equal(7, res)
	s.Equal(3, calls)
}

func (s *RetryTestSuite) TestRetryableCall_GivesUpAfterMaxAttempts() {
	calls := 0
	_, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.Unavailable, "still down")
	})
	s.Require().Error(err)
	s.Equal(3, calls, "should attempt 3 times then stop")
}

func (s *RetryTestSuite) TestRetryableCall_DoesNotRetryNonRetryableError() {
	calls := 0
	_, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.NotFound, "missing")
	})
	s.Require().Error(err)
	s.Equal(1, calls)
}

func (s *RetryTestSuite) TestRetryableCall_RespectsContextCancellation() {
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
	s.Require().Error(err)
	// Could be 1 or 2 depending on timing — never the full 3.
	s.Less(calls, 3)
}

func (s *RetryTestSuite) TestWithTimeout_DerivesDeadline() {
	parent := context.Background()
	ctx, cancel := withTimeout(parent, 100*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	s.True(ok)
	s.WithinDuration(time.Now().Add(100*time.Millisecond), deadline, 50*time.Millisecond)
}

func (s *RetryTestSuite) TestRetryableCall_MetricsDispatchesOnRetry() {
	m := metrics.NewMetrics()
	metrics.RegisterInstance(m)
	defer func() {
		metrics.UnregisterInstance(m)
	}()

	calls := 0
	_, err := retryableCall(context.Background(), "FakeOp", func(ctx context.Context) (int, error) {
		calls++
		if calls == 1 {
			return 0, status.Error(codes.Unavailable, "boom")
		}
		return 42, nil
	})
	s.Require().NoError(err)
	// At least one retry was fired for "FakeOp" / "Unavailable".
	s.Assert().GreaterOrEqual(int(testutil.ToFloat64(m.RetryTotal.WithLabelValues("FakeOp", "Unavailable"))), 1)
}

func TestRetryTestSuite(t *testing.T) {
	suite.Run(t, new(RetryTestSuite))
}
