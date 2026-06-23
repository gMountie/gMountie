package wal

// overlay.go — in-memory pending overlay (base ⊕ pending) for WAL state.
//
// Design: a flat map[string]*overlayNode keyed by full clean path (same
// convention as memfs / cache: root is "", no leading slash, parent + "/" +
// name). The overlay is SPARSE: it only holds paths that have been mutated by
// pending ops. It does NOT synthesise placeholder parent nodes for
// intermediaries that already exist in base.
//
// Tombstones are modelled as ordinary overlayNodes with tomb=true. They appear
// in Paths() (needed for loud-data-loss logging in Task 13) and Stat returns
// ok=true, tombstoned=true so the caller returns ENOENT without consulting base.
//
// Byte-range tracking: each overlayNode maintains a list of written intervals
// in ascending order. ReadMerge uses those intervals to overlay pending bytes
// over base bytes precisely (pending [10,20) over base [0,100) → base[0:10] +
// pending[10:20] + base[20:100]). This also handles pending writes past EOF
// (the result extends beyond base).

import (
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
)

// ── node ──────────────────────────────────────────────────────────────────────

// overlayNode mirrors the memfs node shape (mode/uid/gid/times/version/data/
// target/xattrs) but is embedded in a flat path-keyed map rather than a
// children-tree.
type overlayNode struct {
	ino  uint64
	mode uint32 // full mode incl. type bits

	uid uint32
	gid uint32

	atime, mtime, ctime time.Time

	// version is bumped on each mutation (cheap revalidation support).
	version uint64

	// data holds pending file bytes. Not all bytes are pending — see intervals.
	data []byte

	// intervals is the sorted, coalesced list of byte ranges that are pending.
	// Only bytes covered by an interval override base; bytes outside fall through.
	intervals []byteRange

	// target is the symlink target.
	target string

	// xattrs — pending xattr mutations (not yet backed by a WAL OpKind).
	xattrs map[string][]byte

	// tomb is true when this node represents a TOMBSTONE (pending delete/rename-away).
	tomb bool
}

// byteRange is a half-open byte interval [start, end).
type byteRange struct{ start, end int64 }

func (n *overlayNode) attr() *backend.Attr {
	sz := n.size()
	return &backend.Attr{
		Ino:     n.ino,
		Size:    sz,
		Blocks:  (sz + 511) / 512,
		Atime:   uint64(n.atime.Unix()),
		Mtime:   uint64(n.mtime.Unix()),
		Ctime:   uint64(n.ctime.Unix()),
		Mode:    n.mode,
		Nlink:   1,
		Uid:     n.uid,
		Gid:     n.gid,
		Blksize: 4096,
		Version: n.version,
	}
}

func (n *overlayNode) size() uint64 {
	if n.mode&syscall.S_IFMT == syscall.S_IFLNK {
		return uint64(len(n.target))
	}
	if n.mode&syscall.S_IFMT == syscall.S_IFDIR {
		return 4096
	}
	// For files: the highest byte covered by any interval, or len(data).
	if len(n.intervals) > 0 {
		last := n.intervals[len(n.intervals)-1]
		if last.end > int64(len(n.data)) {
			return uint64(last.end)
		}
	}
	return uint64(len(n.data))
}

func (n *overlayNode) touch() {
	now := time.Now()
	n.mtime = now
	n.ctime = now
	n.version++
}

// addInterval records a new written byte range and coalesces overlapping / adjacent
// intervals in the list. The resulting list is sorted and non-overlapping.
func (n *overlayNode) addInterval(start, end int64) {
	newR := byteRange{start, end}
	merged := make([]byteRange, 0, len(n.intervals)+1)
	inserted := false
	for _, r := range n.intervals {
		if inserted {
			// Try to merge with last merged entry.
			last := &merged[len(merged)-1]
			if r.start <= last.end {
				if r.end > last.end {
					last.end = r.end
				}
			} else {
				merged = append(merged, r)
			}
			continue
		}
		if newR.start <= r.end && r.start <= newR.end {
			// Overlapping or adjacent: merge into newR.
			if r.start < newR.start {
				newR.start = r.start
			}
			if r.end > newR.end {
				newR.end = r.end
			}
		} else if newR.start < r.start {
			// newR comes before r: emit newR first.
			merged = append(merged, newR)
			merged = append(merged, r)
			inserted = true
		} else {
			// r comes before newR: emit r.
			merged = append(merged, r)
		}
	}
	if !inserted {
		merged = append(merged, newR)
	}
	n.intervals = merged
}

// ── Overlay ───────────────────────────────────────────────────────────────────

// Overlay is the in-memory pending state for a delegation holder: a sparse,
// flat map of path → overlayNode, including tombstones for deferred deletes.
// All public methods are concurrency-safe (guarded by mu).
type Overlay struct {
	mu      sync.RWMutex
	nodes   map[string]*overlayNode // path → node (incl. tombstones)
	nextIno uint64
}

// NewOverlay returns an empty, ready-to-use Overlay.
func NewOverlay() *Overlay {
	return &Overlay{
		nodes:   make(map[string]*overlayNode),
		nextIno: 1,
	}
}

func (ov *Overlay) allocIno() uint64 {
	ino := ov.nextIno
	ov.nextIno++
	return ino
}

// newFileNode returns a fresh file overlayNode.
func (ov *Overlay) newFileNode(mode uint32) *overlayNode {
	now := time.Now()
	return &overlayNode{
		ino:    ov.allocIno(),
		mode:   mode,
		atime:  now,
		mtime:  now,
		ctime:  now,
		xattrs: make(map[string][]byte),
	}
}

// newDirNode returns a fresh directory overlayNode.
func (ov *Overlay) newDirNode(mode uint32) *overlayNode {
	now := time.Now()
	return &overlayNode{
		ino:    ov.allocIno(),
		mode:   mode,
		atime:  now,
		mtime:  now,
		ctime:  now,
		xattrs: make(map[string][]byte),
	}
}

// tombNode returns a tombstone overlayNode.
func tombNode() *overlayNode {
	return &overlayNode{tomb: true, ctime: time.Now()}
}

// ── Apply ─────────────────────────────────────────────────────────────────────

// Apply mutates the overlay according to op. Ops are applied in WAL-sequence
// order; Apply is safe for concurrent calls.
func (ov *Overlay) Apply(op Op) {
	ov.mu.Lock()
	defer ov.mu.Unlock()

	switch op.Kind {
	case OpCreate:
		ov.applyCreate(op)
	case OpMkdir:
		ov.applyMkdir(op)
	case OpWrite:
		ov.applyWrite(op)
	case OpUnlink, OpRmdir:
		ov.applyDelete(op)
	case OpRename:
		ov.applyRename(op)
	case OpSetAttr:
		ov.applySetAttr(op)
	case OpSymlink:
		ov.applySymlink(op)
	}
}

func (ov *Overlay) applyCreate(op Op) {
	mode := op.Mode
	if mode == 0 {
		mode = 0o100644
	}
	n := ov.newFileNode(mode)
	ov.nodes[op.Path] = n
}

func (ov *Overlay) applyMkdir(op Op) {
	mode := op.Mode
	if mode == 0 {
		mode = 0o40755
	}
	n := ov.newDirNode(mode)
	ov.nodes[op.Path] = n
}

func (ov *Overlay) applyWrite(op Op) {
	n, ok := ov.nodes[op.Path]
	if !ok || n.tomb {
		// Write to a path not yet in overlay (base-only file) or after a
		// tombstone — create a synthetic node for it.
		n = ov.newFileNode(0o100644)
		ov.nodes[op.Path] = n
	}

	start := op.Offset
	end := start + int64(len(op.Data))
	if end > int64(len(n.data)) {
		grown := make([]byte, end)
		copy(grown, n.data)
		n.data = grown
	}
	copy(n.data[start:end], op.Data)
	n.addInterval(start, end)
	n.touch()
}

func (ov *Overlay) applyDelete(op Op) {
	ov.nodes[op.Path] = tombNode()
}

func (ov *Overlay) applyRename(op Op) {
	// Move any pending nodes whose path is op.Path or is under op.Path/.
	oldRoot := op.Path
	newRoot := op.NewPath

	// Collect paths to re-key.
	type move struct{ old, new string }
	var moves []move
	for p := range ov.nodes {
		if p == oldRoot {
			moves = append(moves, move{p, newRoot})
		} else if strings.HasPrefix(p, oldRoot+"/") {
			moves = append(moves, move{p, newRoot + p[len(oldRoot):]})
		}
	}

	for _, m := range moves {
		n := ov.nodes[m.old]
		ov.nodes[m.new] = n
		if n != nil && !n.tomb {
			n.touch()
		}
		ov.nodes[m.old] = tombNode()
	}

	// If oldRoot had no pending state, still tombstone it.
	if _, ok := ov.nodes[oldRoot]; !ok {
		ov.nodes[oldRoot] = tombNode()
	}
}

func (ov *Overlay) applySetAttr(op Op) {
	n, ok := ov.nodes[op.Path]
	if !ok || n.tomb {
		// SetAttr on a base-only path — create a placeholder.
		n = ov.newFileNode(0o100644)
		ov.nodes[op.Path] = n
	}

	// op.Flags carries backend.FATTR_* bits (same values as WAL Flags field).
	if op.Flags&backend.FATTR_MODE != 0 {
		n.mode = (n.mode & syscall.S_IFMT) | (op.Mode & 0o7777)
	}
	if op.Flags&backend.FATTR_UID != 0 {
		n.uid = op.Mode // re-use Mode field as UID when Flags says so
		// Note: Op carries Mode+Flags only; separate uid/gid fields are a
		// future extension when the WAL Op struct grows them.
	}
	n.touch()
}

func (ov *Overlay) applySymlink(op Op) {
	// OpSymlink: path=linkPath, Data=target bytes (no Op.Target field today).
	now := time.Now()
	n := &overlayNode{
		ino:    ov.allocIno(),
		mode:   0o120777, // S_IFLNK | 0o777
		target: string(op.Data),
		atime:  now,
		mtime:  now,
		ctime:  now,
		xattrs: make(map[string][]byte),
	}
	ov.nodes[op.Path] = n
}

// ── Query methods ─────────────────────────────────────────────────────────────

// Stat returns the pending attrs for path, whether there is any pending state
// (ok), and whether that state is a tombstone (tombstoned). ok=false means no
// overlay entry at all; the caller should consult base. ok=true+tombstoned=true
// means the path was deleted — return ENOENT without consulting base.
func (ov *Overlay) Stat(path string) (attr *backend.Attr, ok bool, tombstoned bool) {
	ov.mu.RLock()
	defer ov.mu.RUnlock()

	n, exists := ov.nodes[path]
	if !exists {
		return nil, false, false
	}
	if n.tomb {
		return nil, true, true
	}
	return n.attr(), true, false
}

// Has returns true if path has any pending state (including a tombstone).
func (ov *Overlay) Has(path string) bool {
	ov.mu.RLock()
	defer ov.mu.RUnlock()
	_, ok := ov.nodes[path]
	return ok
}

// ListMerge produces a merged directory listing for dirPath.
// base entries whose name appears as a tombstone in the overlay are omitted;
// overlay-created entries not already in base are appended.
func (ov *Overlay) ListMerge(dirPath string, base []backend.DirEntryPlus) []backend.DirEntryPlus {
	ov.mu.RLock()
	defer ov.mu.RUnlock()

	// Build a set of base entry names for dedup.
	baseNames := make(map[string]struct{}, len(base))
	for _, e := range base {
		baseNames[e.Name] = struct{}{}
	}

	// Determine which names under dirPath have pending state.
	pendingChildren := make(map[string]*overlayNode)
	for p, n := range ov.nodes {
		parent, name := splitPath2(p)
		if parent == dirPath {
			pendingChildren[name] = n
		}
	}

	// Start with filtered base: drop tombstoned entries.
	out := make([]backend.DirEntryPlus, 0, len(base)+len(pendingChildren))
	for _, e := range base {
		if pn, ok := pendingChildren[e.Name]; ok && pn.tomb {
			continue // omit tombstoned base entry
		}
		out = append(out, e)
	}

	// Append pending-created entries that aren't already in base.
	for name, pn := range pendingChildren {
		if pn.tomb {
			continue // tombstones are not actual entries
		}
		if _, inBase := baseNames[name]; inBase {
			continue // already represented (possibly updated) in base pass above
		}
		fullPath := joinPath2(dirPath, name)
		_ = fullPath // path available for future diagnostic use
		out = append(out, backend.DirEntryPlus{
			DirEntry: backend.DirEntry{
				Ino:  pn.ino,
				Mode: pn.mode,
				Name: name,
			},
			Attr: pn.attr(),
			// XattrListed intentionally false: overlay doesn't track xattr listings.
		})
	}

	return out
}

// ReadMerge overlays pending bytes for path over the base byte slice.
// The read logical offset is off (the start of the base slice in file-space).
// Pending write intervals that intersect [off, off+len(base)) override those
// bytes; pending writes beyond base EOF extend the result.
// Returns base unchanged if there is no pending write state for path.
func (ov *Overlay) ReadMerge(path string, off int64, base []byte) []byte {
	ov.mu.RLock()
	defer ov.mu.RUnlock()

	n, ok := ov.nodes[path]
	if !ok || n.tomb || len(n.intervals) == 0 {
		return base
	}

	baseEnd := off + int64(len(base))

	// Compute the maximum end among all pending intervals.
	maxEnd := baseEnd
	for _, r := range n.intervals {
		if r.end > maxEnd {
			maxEnd = r.end
		}
	}

	var out []byte
	if maxEnd > baseEnd {
		// Result is larger than base: allocate and copy base in.
		out = make([]byte, maxEnd-off)
		copy(out, base)
	} else {
		out = make([]byte, len(base))
		copy(out, base)
	}

	// Overlay each pending interval.
	for _, r := range n.intervals {
		// Intersect interval [r.start, r.end) with our output range [off, off+len(out)).
		srcStart := r.start    // position in file-space
		srcEnd := r.end        // position in file-space
		outStart := srcStart - off
		outEnd := srcEnd - off

		// Clamp to output bounds.
		dataOff := int64(0) // offset into n.data for this interval's start
		if outStart < 0 {
			dataOff = -outStart
			outStart = 0
		}
		if outEnd > int64(len(out)) {
			outEnd = int64(len(out))
		}
		if outStart >= outEnd {
			continue
		}

		// Copy from n.data[r.start-off+dataOff : ...] into out[outStart:outEnd].
		dataSrc := r.start + dataOff
		copy(out[outStart:outEnd], n.data[dataSrc:dataSrc+(outEnd-outStart)])
	}

	return out
}

// DropSubtree removes all pending state for root and every path under root.
// root="" drops everything (full flush). Prefix matching is "/" bounded so
// "drop" does not match "dropx".
func (ov *Overlay) DropSubtree(root string) {
	ov.mu.Lock()
	defer ov.mu.Unlock()

	if root == "" {
		ov.nodes = make(map[string]*overlayNode)
		return
	}

	prefix := root + "/"
	for p := range ov.nodes {
		if p == root || strings.HasPrefix(p, prefix) {
			delete(ov.nodes, p)
		}
	}
}

// Paths returns all paths that have pending state, including tombstones.
// Order is unspecified. Used by Task 13's loud-data-loss logging to enumerate
// every in-flight path at delegation recall.
func (ov *Overlay) Paths() []string {
	ov.mu.RLock()
	defer ov.mu.RUnlock()

	out := make([]string, 0, len(ov.nodes))
	for p := range ov.nodes {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// ── path helpers ──────────────────────────────────────────────────────────────

// splitPath2 splits a clean path into (parent, name). Root "" returns ("", "").
// Mirrors memfs.pathParent / baseName conventions.
func splitPath2(p string) (parent, name string) {
	if p == "" {
		return "", ""
	}
	idx := strings.LastIndex(p, "/")
	if idx < 0 {
		return "", p
	}
	return p[:idx], p[idx+1:]
}

// joinPath2 mirrors memfs.joinPath: root parent "" means no prefix.
func joinPath2(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
