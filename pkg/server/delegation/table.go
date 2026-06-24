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
	owner     string // session ID (for self-access/absorption/ReleaseSession)
	root      string
	principal string // identity principal (for fence key = {principal,volume,epoch})
	volume    string // volume name (for fence key = {principal,volume,epoch})
	epoch     string // client wal-epoch (namespaces gen/fence per wal.db)
	gen       uint64 // delegation generation (monotone per {principal,volume,epoch}; 0 = untagged)
}

// delegationTable is the containment index. Not safe for concurrent use; the
// Arbiter serializes access under its own mutex.
type delegationTable struct {
	entries []entry // invariant: same-owner roots are pairwise non-overlapping.
	// Carving may keep a foreign narrower root alongside a wider root that
	// contains it; such a narrower root is always inserted BEFORE its covering
	// root (new roots append last), so ownerOf's first-match returns the most
	// specific owner.
}

func newDelegationTable() *delegationTable { return &delegationTable{} }

// size returns the number of active delegation entries.
func (t *delegationTable) size() int { return len(t.entries) }

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
//
// principal and volume are stored in the entry for fence-key construction on
// handoff (Task 6); they are NOT used for table containment logic.
func (t *delegationTable) grant(owner, root, principal, volume, epoch string, gen uint64) (granted string, excluded []string, ok bool) {
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
	kept = append(kept, entry{owner: owner, root: root, principal: principal, volume: volume, epoch: epoch, gen: gen})
	t.entries = kept
	sort.Strings(excluded)
	return root, excluded, true
}

// entryForRoot returns the entry with exactly this root, if any.
// Used by the Arbiter to capture {principal, volume, gen} before handoff.
func (t *delegationTable) entryForRoot(root string) (entry, bool) {
	for _, e := range t.entries {
		if e.root == root {
			return e, true
		}
	}
	return entry{}, false
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

// drainOwner drops every entry owned by owner and returns the dropped entries
// so callers can inspect gen/principal/volume for fence-key revocation.
func (t *delegationTable) drainOwner(owner string) []entry {
	var drained []entry
	kept := t.entries[:0]
	for _, e := range t.entries {
		if e.owner == owner {
			drained = append(drained, e)
		} else {
			kept = append(kept, e)
		}
	}
	t.entries = kept
	return drained
}

// releaseOwner drops every entry owned by owner (session reap).
func (t *delegationTable) releaseOwner(owner string) {
	t.drainOwner(owner)
}
