package delegation

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type TableSuite struct{ suite.Suite }

func TestTableSuite(t *testing.T) { suite.Run(t, new(TableSuite)) }

// testGrant is a helper that calls grant with placeholder principal/volume/gen
// so table-only tests stay readable.
func testGrant(t *delegationTable, owner, root string) (string, []string, bool) {
	return t.grant(owner, root, "user-"+owner, "vol", "", 0)
}

func (s *TableSuite) TestGrantDisjointSucceeds() {
	tbl := newDelegationTable()
	g1, _, ok1 := testGrant(tbl, "sessA", "teamA")
	g2, _, ok2 := testGrant(tbl, "sessB", "teamB")
	s.True(ok1)
	s.True(ok2)
	s.Equal("teamA", g1)
	s.Equal("teamB", g2)
}

func (s *TableSuite) TestGrantNarrowsAroundForeignSubtree() {
	tbl := newDelegationTable()
	testGrant(tbl, "sessB", "proj/vendor") // B owns a sub-path
	// A asks for the parent; it must be carved to exclude B's subtree.
	granted, excluded, ok := testGrant(tbl, "sessA", "proj")
	s.True(ok)
	s.Equal("proj", granted)
	s.Equal([]string{"proj/vendor"}, excluded)
}

func (s *TableSuite) TestGrantDeniedWhenContainedByForeign() {
	tbl := newDelegationTable()
	testGrant(tbl, "sessB", "proj")
	_, _, ok := testGrant(tbl, "sessA", "proj/src") // fully inside B's root
	s.False(ok)
}

func (s *TableSuite) TestOwnerOfFindsCoveringRoot() {
	tbl := newDelegationTable()
	testGrant(tbl, "sessA", "proj/src")
	owner, root, ok := tbl.ownerOf("proj/src/main.go")
	s.True(ok)
	s.Equal("sessA", owner)
	s.Equal("proj/src", root)
}

func (s *TableSuite) TestReleaseOwnerClearsAll() {
	tbl := newDelegationTable()
	testGrant(tbl, "sessA", "x")
	testGrant(tbl, "sessA", "y")
	tbl.releaseOwner("sessA")
	_, _, ok := tbl.ownerOf("x/file")
	s.False(ok)
}

func (s *TableSuite) TestReleaseDropsExactRoot() {
	tbl := newDelegationTable()
	testGrant(tbl, "sessA", "a")
	testGrant(tbl, "sessA", "b")
	tbl.release("a")
	// After releasing "a", ownerOf should not find it
	_, _, ok := tbl.ownerOf("a/x")
	s.False(ok)
	// But "b" should still be owned by sessA
	owner, root, ok := tbl.ownerOf("b/x")
	s.True(ok)
	s.Equal("sessA", owner)
	s.Equal("b", root)
}

func (s *TableSuite) TestSameOwnerWiderRootAbsorbsNarrower() {
	tbl := newDelegationTable()
	// Grant sessA a narrower root first
	g1, _, ok1 := testGrant(tbl, "sessA", "proj/src")
	s.True(ok1)
	s.Equal("proj/src", g1)
	// Now grant sessA a wider root that contains it
	g2, _, ok2 := testGrant(tbl, "sessA", "proj")
	s.True(ok2)
	s.Equal("proj", g2)
	// ownerOf should return the wider root (narrower was absorbed)
	owner, root, ok := tbl.ownerOf("proj/src/main.go")
	s.True(ok)
	s.Equal("sessA", owner)
	s.Equal("proj", root)
	// Verify that granting again doesn't leave duplicates
	owner2, root2, ok2 := tbl.ownerOf("proj/other")
	s.True(ok2)
	s.Equal("sessA", owner2)
	s.Equal("proj", root2)
}

func (s *TableSuite) TestVolumeRootContainsEverything() {
	tbl := newDelegationTable()
	// Grant sessA the volume root (empty string)
	g, _, ok := testGrant(tbl, "sessA", "")
	s.True(ok)
	s.Empty(g)
	// ownerOf should find it for any path
	owner, root, ok := tbl.ownerOf("anything/deep/x")
	s.True(ok)
	s.Equal("sessA", owner)
	s.Empty(root)
}

// TestEntryForRootReturnsGenAndKey verifies that entryForRoot exposes the
// principal/volume/gen stored at grant time (used for fence-key construction
// on handoff).
func (s *TableSuite) TestEntryForRootReturnsGenAndKey() {
	tbl := newDelegationTable()
	_, _, ok := tbl.grant("sessA", "proj", "alice", "myvol", "", 7)
	s.Require().True(ok)

	e, found := tbl.entryForRoot("proj")
	s.Require().True(found)
	s.Equal("sessA", e.owner)
	s.Equal("alice", e.principal)
	s.Equal("myvol", e.volume)
	s.Equal(uint64(7), e.gen)
}

func (s *TableSuite) TestEntryForRootMissingReturnsNotFound() {
	tbl := newDelegationTable()
	_, found := tbl.entryForRoot("missing")
	s.False(found)
}
