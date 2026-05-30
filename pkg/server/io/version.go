package io

import "github.com/hanwen/go-fuse/v2/fuse"

// VersionFromAttr packs the freshness-relevant fields of a fuse.Attr
// into a single uint64. (mtime_ns, size, ctime_ns) captures every
// observable change to a file's content or metadata.
//
// The mix uses distinct odd-prime multipliers so equal fields in
// different positions can never cancel each other out (unlike XOR).
// In particular, mtime==ctime after a write no longer collapses the
// version token down to size<<16.
//
// Sub-spec D uses this as the Attr.version sent over the wire. Sub-spec
// C's persisted ChunkRef.Version field (zero in previously-shipped
// caches) will pick up real values once Sub-spec D lands.
func VersionFromAttr(a *fuse.Attr) uint64 {
	if a == nil {
		return 0
	}
	const p1 uint64 = 11400714819323198485 // FNV-adjacent odd prime
	const p2 uint64 = 14181476777654086739 // distinct odd prime
	const p3 uint64 = 16597577988658736719 // distinct odd prime
	mtimeNs := a.Mtime*1_000_000_000 + uint64(a.Mtimensec)
	ctimeNs := a.Ctime*1_000_000_000 + uint64(a.Ctimensec)
	v := mtimeNs*p1 + ctimeNs*p2 + a.Size*p3
	// Map 0 → 1 so the sentinel (nil attr → 0) is never aliased by a real attr.
	if v == 0 {
		return 1
	}
	return v
}
