// Package delegation implements the server-side write-delegation arbiter:
// it tracks which session holds a write-delegation over which subtree, grants
// non-overlapping roots (carving around foreign subtrees), and drives recalls
// on contention. Phase 1 governs coherence only — no durability semantics.
package delegation

import (
	"sort"
	"strings"
)

// contains reports whether root a contains path b (a==b, b under a/, or a=="").
func contains(a, b string) bool {
	if a == "" {
		return true
	}
	return a == b || strings.HasPrefix(b, a+"/")
}

type entry struct {
	owner string
	root  string
}

// delegationTable is the containment index. Not safe for concurrent use; the
// Arbiter serializes access under its own mutex.
type delegationTable struct {
	entries []entry // invariant: roots are pairwise non-overlapping
}

func newDelegationTable() *delegationTable { return &delegationTable{} }

// ownerOf returns the entry whose root contains path, if any.
func (t *delegationTable) ownerOf(path string) (owner, root string, ok bool) {
	for _, e := range t.entries {
		if contains(e.root, path) {
			return e.owner, e.root, true
		}
	}
	return "", "", false
}

// grant tries to grant owner a delegation rooted at root. Rules:
//   - if root is contained by a *foreign* root -> denied (ok=false).
//   - if root contains foreign roots -> granted, with those foreign roots
//     returned as excluded (carve around them).
//   - roots owned by the SAME owner under root are absorbed (re-rooted upward).
func (t *delegationTable) grant(owner, root string) (granted string, excluded []string, ok bool) {
	var kept []entry
	for _, e := range t.entries {
		switch {
		case e.owner == owner && contains(root, e.root):
			// absorbed into the wider same-owner grant; drop it.
			continue
		case e.owner != owner && contains(e.root, root):
			// requested root sits inside a foreign delegation -> deny.
			return "", nil, false
		case e.owner != owner && contains(root, e.root):
			// foreign delegation sits inside the requested root -> carve.
			excluded = append(excluded, e.root)
			kept = append(kept, e)
		default:
			kept = append(kept, e)
		}
	}
	kept = append(kept, entry{owner: owner, root: root})
	t.entries = kept
	sort.Strings(excluded)
	return root, excluded, true
}

// release drops the entry with exactly this root (any owner).
func (t *delegationTable) release(root string) {
	kept := t.entries[:0]
	for _, e := range t.entries {
		if e.root != root {
			kept = append(kept, e)
		}
	}
	t.entries = kept
}

// releaseOwner drops every entry owned by owner (session reap).
func (t *delegationTable) releaseOwner(owner string) {
	kept := t.entries[:0]
	for _, e := range t.entries {
		if e.owner != owner {
			kept = append(kept, e)
		}
	}
	t.entries = kept
}
