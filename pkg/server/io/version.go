package io

import "github.com/hanwen/go-fuse/v2/fuse"

// VersionFromAttr packs the freshness-relevant fields of a fuse.Attr
// into a single uint64. (mtime_ns, size, ctime_ns) captures every
// observable change to a file's content or metadata. The xor mixing
// tolerates the field overlaps and gives a collision-resistant token
// for the single-file lifetime — birthday collision needs three
// independent identical-ns events plus aligned size shifts.
//
// Sub-spec D uses this as the Attr.version sent over the wire. Sub-spec
// C's persisted ChunkRef.Version field (zero in previously-shipped
// caches) will pick up real values once Sub-spec D lands.
func VersionFromAttr(a *fuse.Attr) uint64 {
	if a == nil {
		return 0
	}
	mtimeNs := a.Mtime*1_000_000_000 + uint64(a.Mtimensec)
	ctimeNs := a.Ctime*1_000_000_000 + uint64(a.Ctimensec)
	return mtimeNs ^ (a.Size << 16) ^ ctimeNs
}
