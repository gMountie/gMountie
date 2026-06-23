package delegation

import (
	"path"
	"strings"
	"sync"
)

// writeSet tracks recent write paths and yields the lowest common ancestor
// directory to request a delegation over. Safe for concurrent use.
type writeSet struct {
	mu   sync.Mutex
	ring []string
	n    int
	cap  int
}

func newWriteSet(capacity int) *writeSet { return &writeSet{cap: capacity} }

func (w *writeSet) record(p string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.ring) < w.cap {
		w.ring = append(w.ring, p)
	} else {
		w.ring[w.n%w.cap] = p
	}
	w.n++
}

// root returns the LCA *directory* of the recorded paths ("" == mount root).
func (w *writeSet) root() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.ring) == 0 {
		return ""
	}
	lca := path.Dir(w.ring[0])
	for _, p := range w.ring[1:] {
		lca = commonDir(lca, path.Dir(p))
		if lca == "." || lca == "" {
			return ""
		}
	}
	if lca == "." {
		return ""
	}
	return lca
}

// commonDir returns the longest shared leading path segment sequence of a,b.
func commonDir(a, b string) string {
	if a == b {
		return a
	}
	as, bs := strings.Split(a, "/"), strings.Split(b, "/")
	i := 0
	for i < len(as) && i < len(bs) && as[i] == bs[i] {
		i++
	}
	if i == 0 {
		return ""
	}
	return strings.Join(as[:i], "/")
}
