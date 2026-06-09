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
	// One alloc for the wrapper struct + one for the constant-resolver
	// closure capturing the identity snapshot.
	s.LessOrEqual(allocs, 2.0)
}

func (s *BoundFSSuite) TestCapsRoundTripThroughBoundFS() {
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	id := &Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}, Caps: []string{"dac_override"}}
	bound := NewIdentityBoundFS(base, id).(*resolverBoundFS)
	resolved, err := bound.resolve(bound.principal)
	s.Require().NoError(err)
	s.Equal([]string{"dac_override"}, resolved.Caps)
	s.True(resolved.HasCap("dac_override"))
	s.True(resolved.HasCap("DAC_OVERRIDE"), "HasCap must be case-insensitive")
	s.False(resolved.HasCap("dac_read_search"))
}

// TestIdentityBoundFSSnapshotsIdentity pins the constant-resolver contract:
// the identity is copied at construction, so a caller mutating the original
// *Identity afterwards does not affect the bound FS.
func (s *BoundFSSuite) TestIdentityBoundFSSnapshotsIdentity() {
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	id := &Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}
	bound := NewIdentityBoundFS(base, id).(*resolverBoundFS)
	id.Uid = 4242 // mutate after construction
	resolved, err := bound.resolve("")
	s.Require().NoError(err)
	s.Equal(uint32(1000), resolved.Uid, "identity must be snapshotted at construction")
}
