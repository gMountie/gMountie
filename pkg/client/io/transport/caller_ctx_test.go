// pkg/client/io/caller_ctx_test.go
package transport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

type CallerCtxSuite struct{ suite.Suite }

func TestCallerCtxSuite(t *testing.T) { suite.Run(t, new(CallerCtxSuite)) }

func (s *CallerCtxSuite) TestWithCallerIsReadByCallerFromCtx() {
	ctx := WithCaller(context.Background(), 501, 20, 4242)
	c := callerFromCtx(ctx)
	s.Require().NotNil(c.Owner)
	s.Equal(uint32(501), c.Owner.Uid)
	s.Equal(uint32(20), c.Owner.Gid)
	s.Equal(uint32(4242), c.Pid)
}

func (s *CallerCtxSuite) TestBareContextFallsBackToZeroCaller() {
	c := callerFromCtx(context.Background())
	s.Require().NotNil(c.Owner)
	s.Equal(uint32(0), c.Owner.Uid)
}
