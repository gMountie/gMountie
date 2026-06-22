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
	c.trip("hot", t0)
	s.True(c.cooling("hot", t0.Add(500*time.Millisecond)))   // inside window
	s.False(c.cooling("hot", t0.Add(2*time.Second)))         // window passed
	s.False(c.cooling("cold", t0))                           // never tripped
}

func (s *CooldownSuite) TestExponentialBackoffCaps() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: 4 * time.Second, Cap: 1024})
	t0 := time.Unix(0, 0)
	c.trip("p", t0)                       // window 1s
	c.trip("p", t0.Add(time.Second))      // -> 2s
	c.trip("p", t0.Add(3*time.Second))    // -> 4s
	c.trip("p", t0.Add(7*time.Second))    // -> capped at 4s
	base := t0.Add(7 * time.Second)
	s.True(c.cooling("p", base.Add(3*time.Second)))  // still inside 4s
	s.False(c.cooling("p", base.Add(5*time.Second))) // past capped 4s
}

func (s *CooldownSuite) TestSweepEvictsExpired() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 1024})
	t0 := time.Unix(0, 0)
	c.trip("a", t0)
	c.sweep(t0.Add(time.Hour))
	s.Equal(0, c.len())
}
