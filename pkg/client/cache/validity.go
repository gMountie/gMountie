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
type validityTracker struct {
	state         atomic.Int32        // validityState
	verifiedPaths sync.Map            // path → struct{}, populated during partial revalidation under stateUnverified
}

func newValidityTracker() *validityTracker {
	return &validityTracker{}
}

func (v *validityTracker) globalState() validityState {
	return validityState(v.state.Load())
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
	// A new unverified epoch invalidates the prior epoch's per-path
	// stamps — those stamps are only meaningful within a single
	// disconnect window.
	v.verifiedPaths.Range(func(k, _ any) bool {
		v.verifiedPaths.Delete(k)
		return true
	})
}

func (v *validityTracker) markPathVerified(path string) {
	v.verifiedPaths.Store(path, struct{}{})
}

func (v *validityTracker) isPathVerified(path string) bool {
	_, ok := v.verifiedPaths.Load(path)
	return ok
}
