package delegation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type CooldownSuite struct{ suite.Suite }

func TestCooldownSuite(t *testing.T) { suite.Run(t, new(CooldownSuite)) }

func (s *CooldownSuite) TestTripThenCoolingThenExpires() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 1024})
	t0 := time.Unix(0, 0)
	c.trip("vol", "hot", t0)
	s.True(c.cooling("vol", "hot", t0.Add(500*time.Millisecond))) // inside window
	s.False(c.cooling("vol", "hot", t0.Add(2*time.Second)))       // window passed
	s.False(c.cooling("vol", "cold", t0))                         // never tripped
}

func (s *CooldownSuite) TestExponentialBackoffCaps() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: 4 * time.Second, Cap: 1024})
	t0 := time.Unix(0, 0)
	c.trip("vol", "p", t0)                    // window 1s
	c.trip("vol", "p", t0.Add(time.Second))   // -> 2s
	c.trip("vol", "p", t0.Add(3*time.Second)) // -> 4s
	c.trip("vol", "p", t0.Add(7*time.Second)) // -> capped at 4s
	base := t0.Add(7 * time.Second)
	s.True(c.cooling("vol", "p", base.Add(3*time.Second)))  // still inside 4s
	s.False(c.cooling("vol", "p", base.Add(5*time.Second))) // past capped 4s
}

// TestCooldownIsVolumeScoped: a recall-tripped root on one volume must not
// deny re-grants of the identically-named root on another volume.
func (s *CooldownSuite) TestCooldownIsVolumeScoped() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 1024})
	t0 := time.Unix(0, 0)
	c.trip("vol1", "proj", t0)
	s.True(c.cooling("vol1", "proj", t0.Add(500*time.Millisecond)))
	s.False(c.cooling("vol2", "proj", t0.Add(500*time.Millisecond)),
		"vol1's cooldown must not cool vol2's identically-named root")
}

func (s *CooldownSuite) TestSweepEvictsExpired() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 1024})
	t0 := time.Unix(0, 0)
	c.trip("vol", "a", t0)
	c.sweep(t0.Add(time.Hour))
	s.Equal(0, c.len())
}

func (s *CooldownSuite) TestCapEvictsOldest() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 2})
	t0 := time.Unix(0, 0)
	// Trip three distinct roots: a at t0, b at t0+1s, c at t0+2s
	c.trip("vol", "a", t0)
	c.trip("vol", "b", t0.Add(time.Second))
	c.trip("vol", "c", t0.Add(2*time.Second))
	// Cap is 2, so we expect len == 2 and "a" to have been evicted
	s.Equal(2, c.len())
	// "a" should no longer be cooling (oldest was evicted)
	s.False(c.cooling("vol", "a", t0.Add(3*time.Second)))
	// "b" and "c" should still be cooling (or may have expired depending on the window)
	// but they were the more recent ones
	s.True(c.cooling("vol", "b", t0.Add(time.Second+500*time.Millisecond)))
	s.True(c.cooling("vol", "c", t0.Add(2*time.Second+500*time.Millisecond)))
}
