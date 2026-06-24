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
//
// Base-delta model
// ────────────────
// A node is a "base-delta" when it was synthesised to record a mutation
// (write, setattr, xattr) for a path that was NOT created by this overlay
// (i.e., the path exists only in base). In that case:
//   - baseDelta = true
//   - valid carries the FATTR_* bitmask of which Attr fields the overlay
//     actually modified (OR'd across all applied ops).
//   - Stat returns baseDelta=true and the valid mask; the caller (Task 10)
//     MUST merge: apply only the fields whose FATTR_* bit is set in valid
//     over the base Attr, and keep the base type bits / all unset fields.
//
// A full-create node (baseDelta=false) has an authoritative Attr — the overlay
// created the inode and no base merge is needed.
//
// SIZE note: a plain write on a base-delta node does NOT set FATTR_SIZE in
// valid. The true final size = max(base.Size, overlay.attr.Size). Only an
// explicit SetAttr with FATTR_SIZE is authoritative (a shrink-truncate); in
// that case FATTR_SIZE IS set in valid and attr.Size is the exact target size.
//
// XAttr tombstones: RemoveXAttr on a base xattr cannot simply delete from the
// map — that would fall through to base and lose the removal. The node records
// explicit removals in xattrRemoved. The observable contract (via Xattr):
//   - Xattr(name) → (val, set=true, removed=false)  → pending set value
//   - Xattr(name) → (nil, set=false, removed=true)  → pending removal
//   - Xattr(name) → (nil, set=false, removed=false) → no pending state; consult base

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

	atime, mtime, ctime  time.Time
	atimeNsec, mtimeNsec uint32

	// version is bumped on each mutation (cheap revalidation support).
	version uint64

	// data holds pending file bytes. Not all bytes are pending — see intervals.
	data []byte

	// intervals is the sorted, coalesced list of byte ranges that are pending.
	// Only bytes covered by an interval override base; bytes outside fall through.
	intervals []byteRange

	// target is the symlink target.
	target string

	// xattrs holds pending xattr sets.
	xattrs map[string][]byte

	// xattrRemoved records pending xattr removals (base xattrs removed by this
	// overlay). A name in this set AND in xattrs is undefined — SetXAttr after
	// RemoveXAttr must remove from xattrRemoved and add to xattrs.
	xattrRemoved map[string]struct{}

	// tomb is true when this node represents a TOMBSTONE (pending delete/rename-away).
	tomb bool

	// baseDelta is true when this node was synthesised to record a mutation of
	// a path that exists only in base (not created by this overlay). Task 10
	// must merge the overlay attrs (filtered by valid) over the base Attr.
	baseDelta bool

	// valid is the FATTR_* bitmask of attr fields that this overlay actually
	// modified. Only meaningful when baseDelta=true; for full-create nodes the
	// entire Attr is authoritative.
	valid uint32
}

// byteRange is a half-open byte interval [start, end).
type byteRange struct{ start, end int64 }

func (n *overlayNode) attr() *backend.Attr {
	sz := n.size()
	return &backend.Attr{
		Ino:       n.ino,
		Size:      sz,
		Blocks:    (sz + 511) / 512,
		Atime:     uint64(n.atime.Unix()),
		Atimensec: n.atimeNsec,
		Mtime:     uint64(n.mtime.Unix()),
		Mtimensec: n.mtimeNsec,
		Ctime:     uint64(n.ctime.Unix()),
		Mode:      n.mode,
		Nlink:     1,
		Uid:       n.uid,
		Gid:       n.gid,
		Blksize:   4096,
		Version:   n.version,
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

// clampIntervals removes or trims intervals that extend beyond newSize.
func (n *overlayNode) clampIntervals(newSize int64) {
	out := n.intervals[:0]
	for _, r := range n.intervals {
		if r.start >= newSize {
			continue // entirely past new EOF
		}
		if r.end > newSize {
			r.end = newSize
		}
		out = append(out, r)
	}
	n.intervals = out
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

// newFileNode returns a fresh full-create file overlayNode (baseDelta=false).
func (ov *Overlay) newFileNode(mode uint32) *overlayNode {
	now := time.Now()
	return &overlayNode{
		ino:          ov.allocIno(),
		mode:         mode,
		atime:        now,
		mtime:        now,
		ctime:        now,
		xattrs:       make(map[string][]byte),
		xattrRemoved: make(map[string]struct{}),
	}
}

// newDirNode returns a fresh full-create directory overlayNode (baseDelta=false).
func (ov *Overlay) newDirNode(mode uint32) *overlayNode {
	now := time.Now()
	return &overlayNode{
		ino:          ov.allocIno(),
		mode:         mode,
		atime:        now,
		mtime:        now,
		ctime:        now,
		xattrs:       make(map[string][]byte),
		xattrRemoved: make(map[string]struct{}),
	}
}

// newBaseDeltaNode returns a base-delta placeholder node (baseDelta=true).
// It carries only the mutations explicitly applied to it; it must be merged
// with the base Attr by the caller (Task 10) using the valid bitmask.
func (ov *Overlay) newBaseDeltaNode() *overlayNode {
	now := time.Now()
	return &overlayNode{
		ino:          ov.allocIno(),
		ctime:        now,
		xattrs:       make(map[string][]byte),
		xattrRemoved: make(map[string]struct{}),
		baseDelta:    true,
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
	ov.applyLocked(op)
}

// applyLocked is the body of Apply; the caller must hold ov.mu. Shared by Apply
// and Reset so the two can never diverge.
func (ov *Overlay) applyLocked(op Op) {
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
	case OpSetXAttr:
		ov.applySetXAttr(op)
	case OpRemoveXAttr:
		ov.applyRemoveXAttr(op)
	}
}

// Reset atomically replaces the overlay contents with exactly the state
// produced by applying ops in order. The whole clear+rebuild happens under a
// single ov.mu write-lock, so a concurrent reader never observes a partially
// rebuilt overlay (all-or-nothing). Used by the flush path to drop the flushed
// prefix while preserving ops recorded during the in-flight Apply (which remain
// in the post-truncate log and are passed here as the surviving ops).
func (ov *Overlay) Reset(ops []Op) {
	ov.mu.Lock()
	defer ov.mu.Unlock()
	ov.nodes = make(map[string]*overlayNode, len(ops))
	ov.nextIno = 1
	for _, op := range ops {
		ov.applyLocked(op)
	}
}

func (ov *Overlay) applyCreate(op Op) {
	// The kernel passes Create a bare permission mode (no S_IFMT type bit), so
	// force S_IFREG — otherwise the overlay attr looks like a typeless node and
	// the kernel/cache mis-handles a read-your-own-writes Create.
	perm := op.Mode & 0o7777
	if perm == 0 {
		perm = 0o644
	}
	n := ov.newFileNode(syscall.S_IFREG | perm)
	ov.nodes[op.Path] = n
}

func (ov *Overlay) applyMkdir(op Op) {
	// The kernel passes Mkdir a bare permission mode (no S_IFMT type bit). Force
	// S_IFDIR so the deferred directory's overlay attr is typed correctly —
	// without it the kernel rejects the read-your-own-writes mkdir as a
	// non-directory ("cannot create directory: Input/output error").
	perm := op.Mode & 0o7777
	if perm == 0 {
		perm = 0o755
	}
	n := ov.newDirNode(syscall.S_IFDIR | perm)
	ov.nodes[op.Path] = n
}

func (ov *Overlay) applyWrite(op Op) {
	n, ok := ov.nodes[op.Path]
	if !ok || n.tomb {
		// Write to a path not yet in overlay (base-only file) or after a
		// tombstone — create a base-delta node (baseDelta=true).
		n = ov.newBaseDeltaNode()
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
	// NOTE: a plain write does NOT set FATTR_SIZE in n.valid. For base-delta
	// nodes the final size = max(base.Size, overlay.attr.Size). Only a SetAttr
	// with FATTR_SIZE sets an authoritative size (e.g., a truncate/extend).
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

// applySetAttr applies a SetAttr op to the overlay. op.Valid carries the FATTR_*
// bitmask; only fields whose bit is set are applied. For base-delta nodes, the
// valid mask is OR'd into n.valid so Task 10 knows which fields to merge.
func (ov *Overlay) applySetAttr(op Op) {
	n, ok := ov.nodes[op.Path]
	if !ok || n.tomb {
		// SetAttr on a base-only path — create a base-delta placeholder.
		n = ov.newBaseDeltaNode()
		ov.nodes[op.Path] = n
	}

	if op.Valid&backend.FATTR_MODE != 0 {
		n.mode = (n.mode & syscall.S_IFMT) | (op.Mode & 0o7777)
		n.valid |= backend.FATTR_MODE
	}
	if op.Valid&backend.FATTR_UID != 0 {
		n.uid = op.UID
		n.valid |= backend.FATTR_UID
	}
	if op.Valid&backend.FATTR_GID != 0 {
		n.gid = op.GID
		n.valid |= backend.FATTR_GID
	}
	if op.Valid&backend.FATTR_SIZE != 0 {
		newSize := int64(op.Size)
		// Grow or shrink n.data to the target size.
		if newSize > int64(len(n.data)) {
			grown := make([]byte, newSize)
			copy(grown, n.data)
			n.data = grown
		} else {
			n.data = n.data[:newSize]
		}
		n.clampIntervals(newSize)
		n.valid |= backend.FATTR_SIZE
	}
	if op.Valid&backend.FATTR_ATIME != 0 {
		n.atime = time.Unix(op.AtimeSec, int64(op.AtimeNsec))
		n.atimeNsec = op.AtimeNsec
		n.valid |= backend.FATTR_ATIME
	}
	if op.Valid&backend.FATTR_MTIME != 0 {
		n.mtime = time.Unix(op.MtimeSec, int64(op.MtimeNsec))
		n.mtimeNsec = op.MtimeNsec
		n.valid |= backend.FATTR_MTIME
	}
	n.ctime = time.Now()
	n.version++
}

func (ov *Overlay) applySymlink(op Op) {
	// OpSymlink: path=linkPath, Data=target bytes (no Op.Target field today).
	now := time.Now()
	n := &overlayNode{
		ino:          ov.allocIno(),
		mode:         0o120777, // S_IFLNK | 0o777
		target:       string(op.Data),
		atime:        now,
		mtime:        now,
		ctime:        now,
		xattrs:       make(map[string][]byte),
		xattrRemoved: make(map[string]struct{}),
	}
	ov.nodes[op.Path] = n
}

// applySetXAttr records a pending xattr set. If the xattr was previously
// tombstoned (removed), the removal is cleared and the new value takes over.
func (ov *Overlay) applySetXAttr(op Op) {
	n, ok := ov.nodes[op.Path]
	if !ok || n.tomb {
		n = ov.newBaseDeltaNode()
		ov.nodes[op.Path] = n
	}
	delete(n.xattrRemoved, op.XattrName)
	n.xattrs[op.XattrName] = op.XattrValue
	n.touch()
}

// applyRemoveXAttr records a pending xattr removal. If the xattr had a pending
// set value it is cleared; the name is added to xattrRemoved so the caller does
// not fall through to base.
func (ov *Overlay) applyRemoveXAttr(op Op) {
	n, ok := ov.nodes[op.Path]
	if !ok || n.tomb {
		n = ov.newBaseDeltaNode()
		ov.nodes[op.Path] = n
	}
	delete(n.xattrs, op.XattrName)
	n.xattrRemoved[op.XattrName] = struct{}{}
	n.touch()
}

// ── Query methods ─────────────────────────────────────────────────────────────

// Stat returns the pending attrs for path, whether there is any pending state
// (ok), whether that state is a tombstone (tombstoned), and whether the node is
// a base-delta (baseDelta) along with the FATTR_* valid mask.
//
// Callers must interpret the result as follows:
//
//	ok=false                 → no overlay entry; consult base only.
//	ok=true, tombstoned=true → path was deleted; return ENOENT (do not consult base).
//	ok=true, baseDelta=false → full-create node; attr is authoritative.
//	ok=true, baseDelta=true  → base-delta node; attr carries ONLY the fields
//	                           whose FATTR_* bit is set in valid. The caller
//	                           (Task 10) MUST merge: for each FATTR_* bit set
//	                           in valid, apply attr's field over the base Attr;
//	                           keep all other base fields including type bits.
//
// SIZE note for base-delta nodes: if FATTR_SIZE is set in valid, attr.Size is
// the authoritative target (a SetAttr truncate/extend). If FATTR_SIZE is NOT
// set, the final file size = max(base.Size, attr.Size) — write intervals may
// extend beyond the base EOF; attr.Size reflects only the pending data extent.
func (ov *Overlay) Stat(path string) (attr *backend.Attr, ok bool, tombstoned bool, baseDelta bool, valid uint32) {
	ov.mu.RLock()
	defer ov.mu.RUnlock()

	n, exists := ov.nodes[path]
	if !exists {
		return nil, false, false, false, 0
	}
	if n.tomb {
		return nil, true, true, false, 0
	}
	return n.attr(), true, false, n.baseDelta, n.valid
}

// Xattr returns the pending xattr state for the given attribute name on path.
//   - (val, true, false)  → pending set; val is the value.
//   - (nil, false, true)  → pending removal; do not consult base.
//   - (nil, false, false) → no pending xattr state; consult base.
func (ov *Overlay) Xattr(path, name string) (val []byte, set bool, removed bool) {
	ov.mu.RLock()
	defer ov.mu.RUnlock()

	n, ok := ov.nodes[path]
	if !ok || n.tomb {
		return nil, false, false
	}
	if v, ok := n.xattrs[name]; ok {
		return v, true, false
	}
	if _, ok := n.xattrRemoved[name]; ok {
		return nil, false, true
	}
	return nil, false, false
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
		srcStart := r.start // position in file-space
		srcEnd := r.end     // position in file-space
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
