// Package watermark is the server-side durable store for the WAL replay
// seq-watermark and the revoked delegation-generation set, per (identity,
// volume). The watermark dedups replayed ops (≤ watermark = no-op); the revoked
// gens fence superseded replays. They live in ONE store: if they diverge,
// fencing breaks. Default impl is embedded bbolt (OSS, self-contained); the
// cloud injects a centralized impl via the server option seam.
package watermark

type Key struct{ Identity, Volume string }

type Record struct {
	Watermark   uint64
	RevokedGens []uint64
	// GenHi is the highest delegation generation ever issued for this key.
	// NextGen atomically increments it and returns the new value, guaranteeing
	// that gens are never reused across server restarts.  Zero means no gen has
	// been issued yet; NextGen returns 1 on the first call.  Old records that
	// pre-date this field decode to GenHi=0 and are handled correctly.
	GenHi uint64
}

type Store interface {
	Get(k Key) (Record, error)
	// Advance raises the watermark to max(current, watermark), durable (fsync)
	// before return — the persist-before-ack invariant.
	Advance(k Key, watermark uint64) error
	// RevokeGen records gen as revoked, durable (fsync) before return — the
	// persist-before-handoff invariant.
	RevokeGen(k Key, gen uint64) error
	// NextGen atomically increments GenHi for k and returns the new value,
	// durable (fsync) before return.  The returned gen is guaranteed strictly
	// greater than any gen ever issued for k, across server restarts.
	// gen=0 is never returned (GenHi starts at 0, first call returns 1).
	NextGen(k Key) (uint64, error)
	Close() error
}
