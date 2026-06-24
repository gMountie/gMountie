package watermark

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type StoreSuite struct {
	suite.Suite
	dir string
}

func TestStoreSuite(t *testing.T) { suite.Run(t, new(StoreSuite)) }
func (s *StoreSuite) SetupTest()  { s.dir = s.T().TempDir() }

func (s *StoreSuite) open() Store {
	st, err := OpenBBolt(filepath.Join(s.dir, "wm.db"))
	s.Require().NoError(err)
	return st
}

func (s *StoreSuite) TestAdvanceIsMonotoneAndDurable() {
	k := Key{Identity: "alice", Volume: "vol"}
	st := s.open()
	s.Require().NoError(st.Advance(k, 5))
	s.Require().NoError(st.Advance(k, 3)) // lower ignored
	s.Require().NoError(st.Close())

	st2 := s.open() // reopen → durability
	r, err := st2.Get(k)
	s.Require().NoError(err)
	s.Equal(uint64(5), r.Watermark)
	s.Require().NoError(st2.Close())
}

func (s *StoreSuite) TestRevokeGenPersists() {
	k := Key{Identity: "bob", Volume: "vol"}
	st := s.open()
	s.Require().NoError(st.RevokeGen(k, 7))
	s.Require().NoError(st.RevokeGen(k, 9))
	s.Require().NoError(st.Close())

	st2 := s.open()
	r, err := st2.Get(k)
	s.Require().NoError(err)
	s.ElementsMatch([]uint64{7, 9}, r.RevokedGens)
	s.Require().NoError(st2.Close())
}

// TestEpochNamespacesRecords pins the bug-#2 fix: keys differing only by Epoch
// are fully independent records, so a fresh client wal.db (new epoch) never
// inherits a prior epoch's watermark/revoked-gens (which would dedup-skip its
// ops or wrongly fence it).
func (s *StoreSuite) TestEpochNamespacesRecords() {
	st := s.open()
	defer func() { s.Require().NoError(st.Close()) }()
	e1 := Key{Identity: "alice", Volume: "vol", Epoch: "ep1"}
	e2 := Key{Identity: "alice", Volume: "vol", Epoch: "ep2"}
	legacy := Key{Identity: "alice", Volume: "vol"} // Epoch == ""

	s.Require().NoError(st.Advance(e1, 100))
	s.Require().NoError(st.RevokeGen(e1, 7))

	// e2 (different epoch) is an independent, empty record.
	r2, err := st.Get(e2)
	s.Require().NoError(err)
	s.Zero(r2.Watermark, "fresh epoch must not inherit another epoch's watermark")
	s.Empty(r2.RevokedGens, "fresh epoch must not inherit another epoch's revoked gens")

	// The legacy (epoch="") key is also independent of ep1.
	rl, err := st.Get(legacy)
	s.Require().NoError(err)
	s.Zero(rl.Watermark)

	// ep1 retains its own state, and NextGen is per-epoch.
	r1, err := st.Get(e1)
	s.Require().NoError(err)
	s.Equal(uint64(100), r1.Watermark)
	s.Contains(r1.RevokedGens, uint64(7))

	g1, err := st.NextGen(e1)
	s.Require().NoError(err)
	g2, err := st.NextGen(e2)
	s.Require().NoError(err)
	s.Equal(uint64(1), g2, "each epoch has its own gen counter starting at 1")
	s.NotZero(g1)
}

func (s *StoreSuite) TestGetMissingIsZeroRecord() {
	st := s.open()
	defer func() { s.Require().NoError(st.Close()) }()
	r, err := st.Get(Key{Identity: "nobody", Volume: "v"})
	s.Require().NoError(err)
	s.Equal(uint64(0), r.Watermark)
	s.Empty(r.RevokedGens)
}

// TestNextGenMonotoneAndPerKey verifies that NextGen returns 1,2,3,... and
// that two different keys have independent counters.
func (s *StoreSuite) TestNextGenMonotoneAndPerKey() {
	k1 := Key{Identity: "alice", Volume: "vol"}
	k2 := Key{Identity: "bob", Volume: "vol"}
	st := s.open()
	defer func() { s.Require().NoError(st.Close()) }()

	g1, err := st.NextGen(k1)
	s.Require().NoError(err)
	s.Equal(uint64(1), g1)

	g2, err := st.NextGen(k1)
	s.Require().NoError(err)
	s.Equal(uint64(2), g2)

	g3, err := st.NextGen(k1)
	s.Require().NoError(err)
	s.Equal(uint64(3), g3)

	// k2 is a separate key — starts its own counter at 1.
	gk2, err := st.NextGen(k2)
	s.Require().NoError(err)
	s.Equal(uint64(1), gk2, "distinct keys must have independent counters")
}

// TestNextGenDurableAcrossRestart verifies that reopening the store on the
// same bbolt file (simulating a server restart) continues the counter ABOVE
// the prior max and never resets to 1.
func (s *StoreSuite) TestNextGenDurableAcrossRestart() {
	k := Key{Identity: "carol", Volume: "vol"}

	st := s.open()
	g1, _ := st.NextGen(k)
	g2, _ := st.NextGen(k)
	g3, _ := st.NextGen(k)
	s.Require().NoError(st.Close())

	// "Restart": open the same file.
	st2 := s.open()
	defer func() { s.Require().NoError(st2.Close()) }()

	g4, err := st2.NextGen(k)
	s.Require().NoError(err)
	s.Greater(g4, g3, "post-restart NextGen must be > all pre-restart gens (%d, %d, %d)", g1, g2, g3)
	s.Equal(g3+1, g4, "gen must be strictly sequential across restart (no gap)")
}

// TestNextGenNoFalseFenceAcrossRestart is the regression test for the
// original correctness hole: a gen issued after a restart must NOT collide
// with a durably-revoked gen from before the restart.
//
// Sequence:
//  1. Issue gen G for key K (pre-restart).
//  2. RevokeGen(K, G) — durably marks G as revoked.
//  3. Simulate restart: open a fresh Store on the same file.
//  4. NextGen(K) must return a value > G (no collision with the revoked gen).
//  5. The new gen must NOT appear in RevokedGens (no false-fence).
func (s *StoreSuite) TestNextGenNoFalseFenceAcrossRestart() {
	k := Key{Identity: "dave", Volume: "vault"}

	st := s.open()
	g, err := st.NextGen(k)
	s.Require().NoError(err)
	s.Require().NoError(st.RevokeGen(k, g))
	s.Require().NoError(st.Close())

	// Simulate restart.
	st2 := s.open()
	defer func() { s.Require().NoError(st2.Close()) }()

	newGen, err := st2.NextGen(k)
	s.Require().NoError(err)
	s.Greater(newGen, g, "post-restart gen must be > the revoked gen (no reuse)")

	// The new gen must not be in RevokedGens — that would cause a false-fence.
	rec, err := st2.Get(k)
	s.Require().NoError(err)
	for _, rg := range rec.RevokedGens {
		s.NotEqual(newGen, rg, "new post-restart gen must not appear in RevokedGens")
	}
}
