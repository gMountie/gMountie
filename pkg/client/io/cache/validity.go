package cache

import (
	"sync"
	"sync/atomic"
)

// validityState describes whether cache entries can be trusted without
// a server-side revalidation. The zero value (stateUnverified) is the
// safe default: a freshly-constructed tracker on a freshly-opened
// cachedBackend starts in revalidation mode until the subscribeConsumer
// receives its first HEARTBEAT.
type validityState int32

const (
	stateUnverified validityState = 0
	stateVerified   validityState = 1
)

// validityTracker is the Sub-spec D freshness arbiter. Read paths check
// it before serving cached bytes; subscribeConsumer flips it.
//
// Per-path stamps carry the epoch they were captured in. markGlobalUnverified
// bumps the epoch, which atomically invalidates every prior-epoch stamp without
// touching the map — closing the lost-update race where a concurrent
// markPathVerified (a FUSE-op goroutine still inside revalidate) landed a
// prior-epoch stamp after a non-atomic clear, leaking a stamp into the new
// unverified epoch.
type validityTracker struct {
	state         atomic.Int32  // validityState
	epoch         atomic.Uint64 // bumped by markGlobalUnverified; stamps from a stale epoch are non-authoritative
	verifiedPaths sync.Map      // path → uint64 epoch, populated during partial revalidation under stateUnverified
}

func newValidityTracker() *validityTracker {
	return &validityTracker{}
}

func (v *validityTracker) globalState() validityState {
	return validityState(v.state.Load())
}

// currentEpoch returns the live unverified epoch. revalidate captures this
// BEFORE its GetAttrIfChanged RPC and threads it into markPathVerified, so a
// stamp is only ever attributed to the epoch the revalidation actually began in
// — never a newer epoch a concurrent markGlobalUnverified advanced to mid-flight.
func (v *validityTracker) currentEpoch() uint64 {
	return v.epoch.Load()
}

func (v *validityTracker) markGlobalVerified() {
	v.state.Store(int32(stateVerified))
	// Once we trust the whole cache, per-path stamps are redundant and
	// just consume memory. Clear them.
	v.verifiedPaths.Range(func(k, _ any) bool {
		v.verifiedPaths.Delete(k)
		return true
	})
}

func (v *validityTracker) markGlobalUnverified() {
	v.state.Store(int32(stateUnverified))
	// A new unverified epoch invalidates the prior epoch's per-path stamps —
	// those stamps are only meaningful within a single disconnect window. The
	// epoch bump does this atomically: any stamp carrying the old epoch now
	// fails the equality check in isPathVerified, so no map clear (and no
	// lost-update race against a concurrent markPathVerified) is needed.
	v.epoch.Add(1)
}

// markPathVerified records that path was revalidated during the unverified
// epoch captured at the start of revalidation. The caller (revalidate) passes
// that captured epoch rather than loading it here, so a markGlobalUnverified
// racing the revalidation cannot make a late stamp masquerade as current.
func (v *validityTracker) markPathVerified(path string, epoch uint64) {
	v.verifiedPaths.Store(path, epoch)
}

// isPathVerified reports whether path holds a stamp from the CURRENT unverified
// epoch. A stamp captured before a markGlobalUnverified bump is non-authoritative
// and returns false, forcing a revalidation rather than serving stale attrs.
func (v *validityTracker) isPathVerified(path string) bool {
	e, ok := v.verifiedPaths.Load(path)
	if !ok {
		return false
	}
	stamped, ok := e.(uint64)
	if !ok {
		return false
	}
	return stamped == v.epoch.Load()
}
