package controller

import (
	"context"
	"testing"

	"gmountie/pkg/server/service"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type IdempotencyTestSuite struct {
	suite.Suite
	mgr     service.SessionManager
	session service.Session
}

func (s *IdempotencyTestSuite) SetupTest() {
	s.mgr = service.NewSessionManager(service.SessionManagerOptions{})
	id, err := s.mgr.Create()
	s.Require().NoError(err)
	s.session, err = s.mgr.Get(id)
	s.Require().NoError(err)
}

func (s *IdempotencyTestSuite) TearDownTest() {
	_ = s.mgr.Stop(context.Background())
}

func (s *IdempotencyTestSuite) TestEmptyRequestIDRejected() {
	_, err := withIdempotency(s.session, "", func() (*stubReply, error) {
		return &stubReply{V: 1}, nil
	})
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.InvalidArgument, st.Code())
}

func (s *IdempotencyTestSuite) TestFirstCallExecutesAndCaches() {
	calls := 0
	r1, err := withIdempotency(s.session, "req-1", func() (*stubReply, error) {
		calls++
		return &stubReply{V: 7}, nil
	})
	s.Require().NoError(err)
	s.Assert().Equal(&stubReply{V: 7}, r1)

	r2, err := withIdempotency(s.session, "req-1", func() (*stubReply, error) {
		calls++
		return &stubReply{V: 999}, nil
	})
	s.Require().NoError(err)
	s.Assert().Equal(&stubReply{V: 7}, r2, "second call must return the cached reply")
	s.Assert().Equal(1, calls)
}

func (s *IdempotencyTestSuite) TestErrorNotCached() {
	calls := 0
	fn := func() (*stubReply, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("first fails")
		}
		return &stubReply{V: 42}, nil
	}

	_, err := withIdempotency(s.session, "req-err", fn)
	s.Require().Error(err)

	r, err := withIdempotency(s.session, "req-err", fn)
	s.Require().NoError(err)
	s.Assert().Equal(&stubReply{V: 42}, r)
	s.Assert().Equal(2, calls)
}

type stubReply struct {
	V int
}

func TestIdempotencyTestSuite(t *testing.T) {
	suite.Run(t, new(IdempotencyTestSuite))
}
