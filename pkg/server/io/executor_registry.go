package io

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"

	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/puzpuzpuz/xsync/v3"
	"go.uber.org/zap"
)

// executorRegistry deduplicates identityExecutors process-wide: every volume
// and principal that resolves to the same credential tuple shares ONE pool of
// pre-credentialed worker threads. Bound-FS wrappers are cached per
// (volume, principal) — ten squash volumes mapping to the same uid would
// otherwise each pin their own worker threads for an identical thread state.
type executorRegistry struct {
	// build constructs the executor; a seam so tests can count constructions
	// and inject failures without touching the credential syscall seams.
	build   func(Identity, int) (*identityExecutor, error)
	entries *xsync.MapOf[string, *executorEntry]
}

// executorEntry collapses concurrent first-use of one identity tuple: the
// xsync map hands every caller the same entry, and the sync.Once guarantees
// exactly one build (the Once also publishes exec to the losers).
type executorEntry struct {
	once sync.Once
	exec *identityExecutor
}

func newExecutorRegistry() *executorRegistry {
	return &executorRegistry{
		build:   newIdentityExecutor,
		entries: xsync.NewMapOf[string, *executorEntry](),
	}
}

// executors is the process-wide registry behind NewConstantResolverBoundFS.
var executors = newExecutorRegistry()

// executorFor returns the process-wide executor for a constant identity,
// creating it on first use. Executors live for the process lifetime — their
// worker threads are permanently pinned and credential-switched — so entries
// are never evicted.
//
// Returns nil (with a once-per-identity logged warning) when worker startup
// fails — callers fall back to per-op switching. The failure is cached: a
// credential switch the kernel refused once (insufficient privilege) will not
// start succeeding later in the same process, so there is no retry.
//
// workers <= 0 selects the default pool size, min(4, GOMAXPROCS). The first
// caller's worker count wins for a given identity; the knob is a single
// process-wide config value, so callers are uniform in practice.
func (r *executorRegistry) executorFor(id Identity, workers int) *identityExecutor {
	if workers <= 0 {
		workers = defaultExecutorWorkers()
	}
	entry, _ := r.entries.LoadOrCompute(executorKey(&id), func() *executorEntry {
		return &executorEntry{}
	})
	entry.once.Do(func() {
		exec, err := r.build(id, workers)
		if err != nil {
			log.Log.Warn("identity executor startup failed; falling back to per-op credential switching",
				zap.Uint32("uid", id.Uid), zap.Uint32("gid", id.Gid),
				zap.Int("workers", workers), zap.Error(err))
			return
		}
		entry.exec = exec
	})
	return entry.exec
}

// executorKey collapses an Identity to the registry key: the full credential
// tuple that determines a worker thread's state. Gids are sorted into a copy
// so callers presenting the same group SET share an executor regardless of
// order. Caps are reduced to the applyCreds dispatch mode (none /
// dac_override / dac_read_search) — that mode is the only cap-derived thread
// state; cap names beyond the two recognized ones do not alter the switch.
func executorKey(id *Identity) string {
	gids := append([]uint32(nil), id.Gids...)
	slices.Sort(gids)
	var b strings.Builder
	fmt.Fprintf(&b, "%d:%d:", id.Uid, id.Gid)
	for i, g := range gids {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", g)
	}
	b.WriteByte(':')
	b.WriteString(credModeOf(id))
	return b.String()
}

// credModeOf names the applyCreds branch id dispatches to.
func credModeOf(id *Identity) string {
	switch {
	case id.HasCap(CapDacOverride):
		return CapDacOverride
	case id.HasCap(CapDacReadSearch):
		return CapDacReadSearch
	default:
		return "none"
	}
}

// defaultExecutorWorkers is the pool size when the config knob
// (server.identity.executor_workers) is 0/unset: enough parallelism for
// overlapping FUSE ops without dedicating many permanently-pinned OS threads.
func defaultExecutorWorkers() int {
	return min(4, runtime.GOMAXPROCS(0))
}
