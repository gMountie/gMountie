package io

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/suite"
)

type BoundFSSuite struct{ suite.Suite }

func TestBoundFSSuite(t *testing.T) { suite.Run(t, new(BoundFSSuite)) }

func (s *BoundFSSuite) TestBindIsAllocationCheap() {
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	id := &Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}
	allocs := testing.AllocsPerRun(100, func() {
		_ = NewIdentityBoundFS(base, id)
	})
	s.LessOrEqual(allocs, 1.0)
}
