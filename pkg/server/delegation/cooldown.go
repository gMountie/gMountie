package delegation

import "time"

type cooldownConfig struct {
	Base time.Duration // first cooldown window
	Max  time.Duration // cap on the window
	Cap  int           // max tracked roots (LRU-ish eviction via sweep + cap)
}

type coolEntry struct {
	until  time.Time
	window time.Duration
}

// cooldownTable records recently-recalled roots so the arbiter denies re-grant
// within a growing window. Not safe for concurrent use (Arbiter serializes).
type cooldownTable struct {
	cfg     cooldownConfig
	entries map[string]coolEntry
}

func newCooldownTable(cfg cooldownConfig) *cooldownTable {
	return &cooldownTable{cfg: cfg, entries: make(map[string]coolEntry)}
}

func (c *cooldownTable) len() int { return len(c.entries) }

// trip starts (or extends, exponentially) the cooldown for root.
func (c *cooldownTable) trip(root string, now time.Time) {
	w := c.cfg.Base
	if e, ok := c.entries[root]; ok {
		w = e.window * 2
		if w > c.cfg.Max {
			w = c.cfg.Max
		}
	}
	if len(c.entries) >= c.cfg.Cap {
		c.evictOldest()
	}
	c.entries[root] = coolEntry{until: now.Add(w), window: w}
}

// cooling reports whether root is still within its cooldown window.
func (c *cooldownTable) cooling(root string, now time.Time) bool {
	e, ok := c.entries[root]
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
	var oldestKey string
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
