package io

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReclaimFlagsSuite struct {
	suite.Suite
}

func (s *ReclaimFlagsSuite) TestStripsCreateExclTrunc() {
	in := uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL | syscall.O_TRUNC)
	s.Equal(uint32(syscall.O_RDWR), sanitizeReopenFlags(in))
}

func (s *ReclaimFlagsSuite) TestPreservesAppend() {
	in := uint32(syscall.O_WRONLY | syscall.O_APPEND)
	s.Equal(uint32(syscall.O_WRONLY|syscall.O_APPEND), sanitizeReopenFlags(in))
}

func (s *ReclaimFlagsSuite) TestReadOnlyUnchanged() {
	s.Equal(uint32(syscall.O_RDONLY), sanitizeReopenFlags(uint32(syscall.O_RDONLY)))
}

func (s *ReclaimFlagsSuite) TestStripsCreateKeepsAppend() {
	in := uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_APPEND)
	s.Equal(uint32(syscall.O_WRONLY|syscall.O_APPEND), sanitizeReopenFlags(in))
}

func TestReclaimFlagsSuite(t *testing.T) {
	suite.Run(t, new(ReclaimFlagsSuite))
}
