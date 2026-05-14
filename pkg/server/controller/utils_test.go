package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

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
	s.Equal(uint32(0), c.Caller.Owner.Uid)
	s.Equal(uint32(0), c.Caller.Owner.Gid)
	s.Equal(uint32(0), c.Caller.Pid)
}

func TestCreateContextTestSuite(t *testing.T) {
	suite.Run(t, new(CreateContextTestSuite))
}
