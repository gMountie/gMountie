package metrics

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics is the set of custom client collectors. Construct via NewMetrics
// and register against a Registerer separately so tests use a fresh
// registry without polluting the global one.
type Metrics struct {
	RetryTotal      *prometheus.CounterVec
	InFlight        *prometheus.GaugeVec
	CacheHits       *prometheus.CounterVec
	CacheMisses     *prometheus.CounterVec
	CacheDedupeHits prometheus.Counter

	// Subscribe / revalidation counters (Sub-spec D).
	CacheRevalidations          *prometheus.CounterVec
	SubscribeEventsReceived     *prometheus.CounterVec
	SubscribeStreamState        prometheus.Gauge
	CacheUnverifiedDurationSecs prometheus.Counter

	// Persist GC / disk-accounting observability (review blind spot #4).
	ChunksUnlinked      *prometheus.CounterVec
	GhostEntriesDeleted prometheus.Counter
	RefcountUnderflows  prometheus.Counter
	OrphansReclaimed    prometheus.Counter
	TmpReclaimed        prometheus.Counter
	BudgetEvictions     prometheus.Counter
	DiskBytesUsed       prometheus.Gauge
}

// NewMetrics constructs the set of client collectors. They are NOT
// registered — call MustRegister or Register on a chosen Registerer.
func NewMetrics() *Metrics {
	return &Metrics{
		RetryTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_client_retry_total",
			Help: "Count of client RPC retries per op and grpc status code.",
		}, []string{"op", "code"}),
		InFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gmountie_client_in_flight",
			Help: "Number of in-flight client RPCs per op.",
		}, []string{"op"}),
		CacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_cache_hits_total",
			Help: "Cache hits, labelled by tier (memory|disk) and sub-cache type (attr|dir|data).",
		}, []string{"tier", "type"}),
		CacheMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_cache_misses_total",
			Help: "Cache misses (both memory and disk missed), labelled by sub-cache type (attr|dir|data).",
		}, []string{"type"}),
		CacheDedupeHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gmountie_cache_dedupe_hits_total",
			Help: "Content-addressable chunks whose hash already existed on disk when WriteChunk ran.",
		}),
		CacheRevalidations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_cache_revalidations_total",
			Help: "GetAttrIfChanged outcomes from the cache revalidation path.",
		}, []string{"result"}),
		SubscribeEventsReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_subscribe_events_received_total",
			Help: "Subscribe events received and applied to the local cache.",
		}, []string{"kind"}),
		SubscribeStreamState: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gmountie_subscribe_stream_state",
			Help: "1 = stream up and verified; 0 = down or unverified.",
		}),
		CacheUnverifiedDurationSecs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gmountie_cache_unverified_duration_seconds_total",
			Help: "Cumulative time the cache spent in unverified mode.",
		}),
		ChunksUnlinked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_cache_chunks_unlinked_total",
			Help: "Persist chunk files removed from disk, labelled by reason (refcount_zero|ghost|orphan|budget).",
		}, []string{"reason"}),
		GhostEntriesDeleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gmountie_cache_ghost_entries_deleted_total",
			Help: "data_idx entries reclaimed by the ghost sweep (index entry over a missing chunk file).",
		}),
		RefcountUnderflows: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gmountie_cache_refcount_underflows_total",
			Help: "Chunk refcount decrements that landed on an absent key (corruption signal).",
		}),
		OrphansReclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gmountie_cache_orphans_reclaimed_total",
			Help: "Unreferenced chunk files reclaimed by the orphan sweep.",
		}),
		TmpReclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gmountie_cache_tmp_reclaimed_total",
			Help: "Crash-leftover .tmp- chunk files reclaimed by the orphan sweep.",
		}),
		BudgetEvictions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gmountie_cache_budget_evictions_total",
			Help: "data_idx entries evicted under disk-budget pressure.",
		}),
		DiskBytesUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gmountie_cache_disk_bytes_used",
			Help: "Currently-accounted bytes in the persist chunks/ tree.",
		}),
	}
}

// MustRegister registers all collectors with r. Panics on registration error.
func (m *Metrics) MustRegister(r prometheus.Registerer) {
	r.MustRegister(
		m.RetryTotal, m.InFlight, m.CacheHits, m.CacheMisses, m.CacheDedupeHits,
		m.CacheRevalidations, m.SubscribeEventsReceived, m.SubscribeStreamState,
		m.CacheUnverifiedDurationSecs,
		m.ChunksUnlinked, m.GhostEntriesDeleted, m.RefcountUnderflows,
		m.OrphansReclaimed, m.TmpReclaimed, m.BudgetEvictions, m.DiskBytesUsed,
	)
}

// Register tolerates AlreadyRegisteredError by adopting the existing
// collector so increments still land in the registered series. Binaries
// that build a client more than once per process (or tests running with
// count>1) stay consistent.
func (m *Metrics) Register(r prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		m.RetryTotal, m.InFlight, m.CacheHits, m.CacheMisses, m.CacheDedupeHits,
		m.CacheRevalidations, m.SubscribeEventsReceived, m.SubscribeStreamState,
		m.CacheUnverifiedDurationSecs,
		m.ChunksUnlinked, m.GhostEntriesDeleted, m.RefcountUnderflows,
		m.OrphansReclaimed, m.TmpReclaimed, m.BudgetEvictions, m.DiskBytesUsed,
	}
	for _, c := range collectors {
		if err := r.Register(c); err != nil {
			var ar prometheus.AlreadyRegisteredError
			if !errors.As(err, &ar) {
				return errors.Wrap(err, "register client metrics")
			}
			// Adopt the previously-registered collector so future
			// calls through m hit the same instance.
			switch existing := ar.ExistingCollector.(type) {
			case *prometheus.CounterVec:
				switch c {
				case prometheus.Collector(m.RetryTotal):
					m.RetryTotal = existing
				case prometheus.Collector(m.CacheHits):
					m.CacheHits = existing
				case prometheus.Collector(m.CacheMisses):
					m.CacheMisses = existing
				case prometheus.Collector(m.CacheRevalidations):
					m.CacheRevalidations = existing
				case prometheus.Collector(m.SubscribeEventsReceived):
					m.SubscribeEventsReceived = existing
				case prometheus.Collector(m.ChunksUnlinked):
					m.ChunksUnlinked = existing
				}
			case *prometheus.GaugeVec:
				if c == prometheus.Collector(m.InFlight) {
					m.InFlight = existing
				}
			// prometheus.Gauge embeds prometheus.Counter, so the Gauge
			// case must precede Counter — otherwise the Counter arm
			// silently catches Gauges.
			case prometheus.Gauge:
				switch c {
				case prometheus.Collector(m.SubscribeStreamState):
					m.SubscribeStreamState = existing
				case prometheus.Collector(m.DiskBytesUsed):
					m.DiskBytesUsed = existing
				}
			case prometheus.Counter:
				switch c {
				case prometheus.Collector(m.CacheDedupeHits):
					m.CacheDedupeHits = existing
				case prometheus.Collector(m.CacheUnverifiedDurationSecs):
					m.CacheUnverifiedDurationSecs = existing
				case prometheus.Collector(m.GhostEntriesDeleted):
					m.GhostEntriesDeleted = existing
				case prometheus.Collector(m.RefcountUnderflows):
					m.RefcountUnderflows = existing
				case prometheus.Collector(m.OrphansReclaimed):
					m.OrphansReclaimed = existing
				case prometheus.Collector(m.TmpReclaimed):
					m.TmpReclaimed = existing
				case prometheus.Collector(m.BudgetEvictions):
					m.BudgetEvictions = existing
				}
			}
		}
	}
	return nil
}

func (m *Metrics) RetryInc(op, code string) { m.RetryTotal.WithLabelValues(op, code).Inc() }
func (m *Metrics) InFlightInc(op string)    { m.InFlight.WithLabelValues(op).Inc() }
func (m *Metrics) InFlightDec(op string)    { m.InFlight.WithLabelValues(op).Dec() }

// CacheHitInc bumps the cache-hits counter for the given tier and sub-cache type.
func (m *Metrics) CacheHitInc(tier, cacheType string) {
	m.CacheHits.WithLabelValues(tier, cacheType).Inc()
}

// CacheMissInc bumps the cache-misses counter for the given sub-cache type.
func (m *Metrics) CacheMissInc(cacheType string) {
	m.CacheMisses.WithLabelValues(cacheType).Inc()
}

// CacheDedupeHitInc bumps the dedupe-hits counter.
func (m *Metrics) CacheDedupeHitInc() { m.CacheDedupeHits.Inc() }

// CacheRevalidationInc bumps the revalidation counter for the given result label.
// result is one of: "not_modified", "changed", "enoent", "error".
func (m *Metrics) CacheRevalidationInc(result string) {
	m.CacheRevalidations.WithLabelValues(result).Inc()
}

// SubscribeEventReceivedInc bumps the subscribe-events-received counter for
// the given kind label. kind is one of: "mutated", "deleted", "renamed", "heartbeat".
func (m *Metrics) SubscribeEventReceivedInc(kind string) {
	m.SubscribeEventsReceived.WithLabelValues(kind).Inc()
}

// SubscribeStreamStateSet sets the stream-state gauge: 1 when up+verified, 0 when down/unverified.
func (m *Metrics) SubscribeStreamStateSet(up bool) {
	if up {
		m.SubscribeStreamState.Set(1)
	} else {
		m.SubscribeStreamState.Set(0)
	}
}

// CacheUnverifiedAdd accumulates the given number of seconds into the
// unverified-duration counter.
func (m *Metrics) CacheUnverifiedAdd(seconds float64) {
	m.CacheUnverifiedDurationSecs.Add(seconds)
}

// --- Persist GC / disk-accounting setters (blind spot #4) ---

// ChunkUnlinkedInc bumps the chunks-unlinked counter for the given reason
// (refcount_zero|ghost|orphan|budget).
func (m *Metrics) ChunkUnlinkedInc(reason string) { m.ChunksUnlinked.WithLabelValues(reason).Inc() }

// GhostEntryDeletedInc bumps the ghost-entries-deleted counter.
func (m *Metrics) GhostEntryDeletedInc() { m.GhostEntriesDeleted.Inc() }

// RefcountUnderflowInc bumps the refcount-underflow counter.
func (m *Metrics) RefcountUnderflowInc() { m.RefcountUnderflows.Inc() }

// OrphanReclaimedInc bumps the orphans-reclaimed counter.
func (m *Metrics) OrphanReclaimedInc() { m.OrphansReclaimed.Inc() }

// TmpReclaimedInc bumps the tmp-reclaimed counter.
func (m *Metrics) TmpReclaimedInc() { m.TmpReclaimed.Inc() }

// BudgetEvictionInc bumps the budget-evictions counter.
func (m *Metrics) BudgetEvictionInc() { m.BudgetEvictions.Inc() }

// DiskBytesUsedSet sets the disk-bytes-used gauge.
func (m *Metrics) DiskBytesUsedSet(n int64) { m.DiskBytesUsed.Set(float64(n)) }

// --- Per-instance hook registry ---
//
// The dispatchers below call every registered *Metrics instance so that
// multi-client processes (including tests building multiple clients in one
// process) do not cross-wire. Each NewClientFromConfig registers its
// private *Metrics via RegisterInstance; the dispatchers fan-out to all
// of them. Protected by instancesMu.
//
// Entries are deduplicated by UNDERLYING COLLECTOR, not by wrapper pointer:
// when two Metrics values registered against the same prometheus Registerer,
// the second adopted the first's collectors (AlreadyRegisteredError handling
// in Register), so fanning out to both would increment the same CounterVec
// twice per event. Entries are refcounted so the fan-out survives until the
// LAST client sharing the collectors has closed.

var (
	instancesMu sync.RWMutex
	instances   []*instanceEntry
)

// instanceEntry refcounts one distinct collector set in the dispatcher.
type instanceEntry struct {
	m    *Metrics
	refs int
}

// sameCollectors reports whether two Metrics share the same underlying
// collectors. RetryTotal is the proxy: Register's AlreadyRegisteredError
// adoption replaces all collectors together, so comparing one is enough.
func sameCollectors(a, b *Metrics) bool {
	return a == b || a.RetryTotal == b.RetryTotal
}

// RegisterInstance adds m to the global dispatcher. A Metrics whose
// collectors are already registered (same pointer, or a wrapper that adopted
// an existing collector set via Register) only bumps the refcount — the
// fan-out increments each underlying collector exactly once per event.
// Called by the client factory after Register() succeeds so the prometheus
// series are in place; paired with UnregisterInstance from ClientImpl.Close.
func RegisterInstance(m *Metrics) {
	instancesMu.Lock()
	defer instancesMu.Unlock()
	for _, e := range instances {
		if sameCollectors(e.m, m) {
			e.refs++
			return
		}
	}
	instances = append(instances, &instanceEntry{m: m, refs: 1})
}

// UnregisterInstance drops one reference to m's collector set, removing the
// entry from the dispatcher when the last reference is gone. Called by
// ClientImpl.Close so closed clients stop receiving fan-out (and tests can
// clean up instances registered within a single test case).
func UnregisterInstance(m *Metrics) {
	instancesMu.Lock()
	defer instancesMu.Unlock()
	for i, e := range instances {
		if sameCollectors(e.m, m) {
			e.refs--
			if e.refs <= 0 {
				instances = append(instances[:i], instances[i+1:]...)
			}
			return
		}
	}
}

// OnRetry fires RetryInc on all registered instances. Called by the
// io-layer retry helper; living in this package avoids an import cycle
// (pkg/client/io → pkg/client/grpc → pkg/client/metrics).
func OnRetry(op, code string) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	for _, e := range instances {
		e.m.RetryInc(op, code)
	}
}

// CacheHit fires CacheHitInc on all registered instances.
func CacheHit(tier, cacheType string) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	for _, e := range instances {
		e.m.CacheHitInc(tier, cacheType)
	}
}

// CacheMiss fires CacheMissInc on all registered instances.
func CacheMiss(cacheType string) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	for _, e := range instances {
		e.m.CacheMissInc(cacheType)
	}
}

// CacheDedupeHit fires CacheDedupeHitInc on all registered instances.
func CacheDedupeHit() {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	for _, e := range instances {
		e.m.CacheDedupeHitInc()
	}
}

// CacheRevalidation fires CacheRevalidationInc on all registered instances.
func CacheRevalidation(result string) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	for _, e := range instances {
		e.m.CacheRevalidationInc(result)
	}
}

// SubscribeEventReceived fires SubscribeEventReceivedInc on all registered instances.
func SubscribeEventReceived(kind string) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	for _, e := range instances {
		e.m.SubscribeEventReceivedInc(kind)
	}
}

// SubscribeStreamStateChanged fires SubscribeStreamStateSet on all registered instances.
func SubscribeStreamStateChanged(up bool) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	for _, e := range instances {
		e.m.SubscribeStreamStateSet(up)
	}
}

// CacheUnverifiedElapsed fires CacheUnverifiedAdd on all registered instances.
func CacheUnverifiedElapsed(seconds float64) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	for _, e := range instances {
		e.m.CacheUnverifiedAdd(seconds)
	}
}
