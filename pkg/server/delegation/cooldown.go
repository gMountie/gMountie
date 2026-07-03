package delegation

import "time"

// CooldownConfigDefault returns the production-default cooldown parameters:
// Base 1 s, Max 60 s, Cap 4096 tracked roots.
func CooldownConfigDefault() cooldownConfig {
	return cooldownConfig{
		Base: time.Second,
		Max:  60 * time.Second,
		Cap:  4096,
	}
}

type cooldownConfig struct {
	Base time.Duration // first cooldown window
	Max  time.Duration // cap on the window
	Cap  int           // max tracked roots (LRU-ish eviction via sweep + cap)
}

type coolEntry struct {
	until  time.Time
	window time.Duration
}

// coolKey identifies a cooling root within its volume — cooldowns, like the
// delegation table, never cross volumes (a recall storm on vol1's "proj" must
// not deny re-grants of vol2's "proj").
type coolKey struct {
	volume string
	root   string
}

// cooldownTable records recently-recalled (volume, root) pairs so the arbiter
// denies re-grant within a growing window. Not safe for concurrent use
// (Arbiter serializes).
type cooldownTable struct {
	cfg     cooldownConfig
	entries map[coolKey]coolEntry
}

func newCooldownTable(cfg cooldownConfig) *cooldownTable {
	return &cooldownTable{cfg: cfg, entries: make(map[coolKey]coolEntry)}
}

func (c *cooldownTable) len() int { return len(c.entries) }

// trip starts (or extends, exponentially) the cooldown for root on volume.
func (c *cooldownTable) trip(volume, root string, now time.Time) {
	k := coolKey{volume: volume, root: root}
	w := c.cfg.Base
	if e, ok := c.entries[k]; ok {
		w = e.window * 2
		if w > c.cfg.Max {
			w = c.cfg.Max
		}
	}
	if len(c.entries) >= c.cfg.Cap {
		c.evictOldest()
	}
	c.entries[k] = coolEntry{until: now.Add(w), window: w}
}

// cooling reports whether root on volume is still within its cooldown window.
func (c *cooldownTable) cooling(volume, root string, now time.Time) bool {
	e, ok := c.entries[coolKey{volume: volume, root: root}]
	if !ok {
		return false
	}
	return now.Before(e.until)
}

// sweep drops entries whose window has fully elapsed.
func (c *cooldownTable) sweep(now time.Time) {
	for k, e := range c.entries {
		if !now.Before(e.until) {
			delete(c.entries, k)
		}
	}
}

func (c *cooldownTable) evictOldest() {
	var oldestKey coolKey
	var oldest time.Time
	first := true
	for k, e := range c.entries {
		if first || e.until.Before(oldest) {
			oldestKey, oldest, first = k, e.until, false
		}
	}
	if !first {
		delete(c.entries, oldestKey)
	}
}
