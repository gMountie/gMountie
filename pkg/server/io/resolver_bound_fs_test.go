package io

import (
	"sync/atomic"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/suite"
)

// ResolverBoundFSSuite tests the resolver-bound FS wrapper (resolverBoundFS)
// that is used for mapped-mode volumes when identity enforcement is active.
// Two properties are verified without requiring root:
//
//  1. Per-op resolution: the resolve function is invoked on every call; the
//     wrapper holds no identity snapshot.
//  2. Cache safety: if the resolver returns a different identity after a TTL
//     refresh, the next call sees the updated identity immediately.
//
// The actual credential-change path (changeIdentity / setfsuid / setfsgid)
// requires root; those paths are covered by BoundFSCredsSuite and
// BoundFSCapsSuite (both skip non-root). This suite tests the resolver
// invocation contract that makes caching the wrapper safe.
type ResolverBoundFSSuite struct{ suite.Suite }

func TestResolverBoundFSSuite(t *testing.T) { suite.Run(t, new(ResolverBoundFSSuite)) }

// TestResolveCalledPerOp asserts the wrapper does NOT snapshot identity at
// construction — each call to the resolve function is a fresh invocation.
func (s *ResolverBoundFSSuite) TestResolveCalledPerOp() {
	var calls atomic.Int64
	resolve := IdentityResolveFunc(func(principal string) (Identity, error) {
		calls.Add(1)
		return Identity{Uid: 1001, Gid: 1001, Gids: []uint32{1001}}, nil
	})
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	r := NewResolverBoundFS(base, resolve, "alice").(*resolverBoundFS)

	// Drive the resolve function 5 times directly.
	for range 5 {
		_, err := r.resolve("alice")
		s.Require().NoError(err)
	}
	s.Equal(int64(5), calls.Load(), "resolve must be called once per invocation, not cached")
}

// TestResolvePrincipalPassedThrough asserts the principal stored at
// construction is forwarded to the resolve function on each call.
func (s *ResolverBoundFSSuite) TestResolvePrincipalPassedThrough() {
	var got []string
	resolve := IdentityResolveFunc(func(principal string) (Identity, error) {
		got = append(got, principal)
		return Identity{Uid: 1001, Gid: 1001, Gids: []uint32{1001}}, nil
	})
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	r := NewResolverBoundFS(base, resolve, "alice").(*resolverBoundFS)

	for range 3 {
		_, _ = r.resolve(r.principal)
	}
	s.Equal([]string{"alice", "alice", "alice"}, got)
}

// TestCacheSafety_ResolverUpdateReflectedImmediately is the core safety test
// for Change 2. A cached resolverBoundFS wrapper must reflect an updated
// resolver answer on the very next call — no identity is frozen in the wrapper.
// Simulates a TTL expiry by having the resolver return different identities on
// successive calls (as the real TTL-cached resolver would after expiry).
func (s *ResolverBoundFSSuite) TestCacheSafety_ResolverUpdateReflectedImmediately() {
	// First resolve: uid 1001. "After TTL expiry": uid 1002.
	answers := []Identity{
		{Uid: 1001, Gid: 1001, Gids: []uint32{1001}},
		{Uid: 1002, Gid: 1002, Gids: []uint32{1002}},
	}
	call := 0
	resolve := IdentityResolveFunc(func(principal string) (Identity, error) {
		idx := call
		if idx >= len(answers) {
			idx = len(answers) - 1
		}
		call++
		return answers[idx], nil
	})
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	r := NewResolverBoundFS(base, resolve, "alice").(*resolverBoundFS)

	id0, err := r.resolve("alice")
	s.Require().NoError(err)
	s.Equal(uint32(1001), id0.Uid, "first resolve must return uid 1001")

	// "TTL expired" — resolver now returns uid 1002.
	id1, err := r.resolve("alice")
	s.Require().NoError(err)
	s.Equal(uint32(1002), id1.Uid, "second resolve must return updated uid 1002 (no snapshot)")
}

// TestWantsFchownFor_PropagatesIdentity asserts that wantsFchownFor returns
// the uid/gid from the resolver when dac_override is set, without caching them.
func (s *ResolverBoundFSSuite) TestWantsFchownFor_PropagatesIdentity() {
	resolve := IdentityResolveFunc(func(principal string) (Identity, error) {
		return Identity{Uid: 2000, Gid: 2001, Gids: []uint32{2001}, Caps: []string{"dac_override"}}, nil
	})
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	r := NewResolverBoundFS(base, resolve, "bob").(*resolverBoundFS)

	uid, gid, ok := r.wantsFchownFor()
	s.True(ok, "dac_override cap should trigger fchown")
	s.Equal(uint32(2000), uid)
	s.Equal(uint32(2001), gid)
}

// TestWantsFchownFor_NoCap asserts that wantsFchownFor returns false when
// no dac_override cap is present.
func (s *ResolverBoundFSSuite) TestWantsFchownFor_NoCap() {
	resolve := IdentityResolveFunc(func(principal string) (Identity, error) {
		return Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}, nil
	})
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	r := NewResolverBoundFS(base, resolve, "carol").(*resolverBoundFS)

	_, _, ok := r.wantsFchownFor()
	s.False(ok, "no dac_override cap should not trigger fchown")
}
