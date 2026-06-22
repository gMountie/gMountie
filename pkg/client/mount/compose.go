package mount

import (
	"sort"

	"go.gmountie.dev/gmountie/pkg/client/io"
)

// layerPos orders backend layers from outermost (closest to FUSE) to innermost
// (the transport leaf). Layers declare a NAMED position; the stack cannot be
// misordered by an index. The writeBatcher/WAL slot (posWritePath) is reserved
// and unused today.
type layerPos int

const (
	posObserver  layerPos = iota // metrics / tracing / audit — outermost
	posCache                     // read/attr/dir/data cache
	posWritePath                 // writeBatcher / WAL slot (reserved; empty now)
	posTransport                 // the gRPC leaf — innermost, always present
)

// backendLayer is one optional layer at a named position.
type backendLayer struct {
	pos   layerPos
	build func(inner io.FileSystemBackend) io.FileSystemBackend
}

// composeBackend wraps transport (innermost) with each layer, innermost-first,
// so the result is node -> observer -> cache -> [writePath] -> transport.
func composeBackend(transport io.FileSystemBackend, layers []backendLayer) io.FileSystemBackend {
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
