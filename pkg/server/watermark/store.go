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
}

type Store interface {
	Get(k Key) (Record, error)
	// Advance raises the watermark to max(current, watermark), durable (fsync)
	// before return — the persist-before-ack invariant.
	Advance(k Key, watermark uint64) error
	// RevokeGen records gen as revoked, durable (fsync) before return — the
	// persist-before-handoff invariant.
	RevokeGen(k Key, gen uint64) error
	Close() error
}
