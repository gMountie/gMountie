package controller

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/server/principal"

	"github.com/stretchr/testify/suite"
)

// testAuthedCtx returns a context carrying the given principal — mirrors what
// the auth interceptor injects at runtime. Use this in handler unit tests
// instead of context.Background() when the session was created with a
// non-empty principal, so the resolveSession ownership check passes.
func testAuthedCtx(p string) context.Context {
	return principal.WithPrincipal(context.Background(), p)
}

type CreateContextTestSuite struct {
	suite.Suite
}

func (s *CreateContextTestSuite) TestCreateContext_NilCallerDoesNotPanic() {
	defer func() {
		if r := recover(); r != nil {
			s.T().Fatalf("createContext panicked on nil caller: %v", r)
		}
	}()
	c := createContext(context.Background(), nil)
	s.NotNil(c)
	s.Equal(uint32(0), c.Uid)
	s.Equal(uint32(0), c.Gid)
	s.Equal(uint32(0), c.Pid)
}

func TestCreateContextTestSuite(t *testing.T) {
	suite.Run(t, new(CreateContextTestSuite))
}
