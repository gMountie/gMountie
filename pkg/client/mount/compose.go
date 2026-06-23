package mount

import (
	"sort"

	"go.gmountie.dev/gmountie/pkg/client/backend"
)

// layerPos orders backend layers from outermost (closest to FUSE) to innermost
// (the transport leaf). Layers declare a NAMED position; the stack cannot be
// misordered by an index. The writeBatcher/WAL slot (posWritePath) is reserved
// and unused today.
type layerPos int

const (
	posIdentity  layerPos = iota // uid/gid rewrite — OUTERMOST (above observer+cache)
	posObserver                  // metrics / tracing / audit
	posWAL                       // WAL layer — base ⊕ pending read-your-own-writes, OUTER of cache
	posCache                     // read/attr/dir/data cache
	posWritePath                 // writeBatcher slot (reserved; empty now)
	posTransport                 // the gRPC leaf — innermost, always present
)

// backendLayer is one optional layer at a named position.
type backendLayer struct {
	pos   layerPos
	build func(inner backend.FileSystemBackend) backend.FileSystemBackend
}

// composeBackend wraps transport (innermost) with each layer, innermost-first,
// so the result is node -> identity -> observer -> WAL -> cache -> [writePath] ->
// transport. The identity layer sits OUTERMOST so the cache (and the Subscribe
// invalidation stream it consumes) keeps storing SERVER ids while the node/
// cgofs adapters see LOCAL display ids. The WAL layer sits OUTER of cache so
// pending writes are visible to all reads before the cache is consulted.
func composeBackend(transport backend.FileSystemBackend, layers []backendLayer) backend.FileSystemBackend {
	sorted := make([]backendLayer, len(layers))
	copy(sorted, layers)
	// Build innermost-first: higher pos value = closer to transport = built earlier.
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].pos > sorted[j].pos })
	result := transport
	for _, l := range sorted {
		result = l.build(result)
	}
	return result
}
