package metrics

import (
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
	}
}

// MustRegister registers all collectors with r. Panics on registration error.
func (m *Metrics) MustRegister(r prometheus.Registerer) {
	r.MustRegister(m.RetryTotal, m.InFlight, m.CacheHits, m.CacheMisses, m.CacheDedupeHits)
}

// Register tolerates AlreadyRegisteredError by adopting the existing
// collector so increments still land in the registered series. Binaries
// that build a client more than once per process (or tests running with
// count>1) stay consistent.
func (m *Metrics) Register(r prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		m.RetryTotal, m.InFlight, m.CacheHits, m.CacheMisses, m.CacheDedupeHits,
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
				}
			case prometheus.Counter:
				if c == prometheus.Collector(m.CacheDedupeHits) {
					m.CacheDedupeHits = existing
				}
			case *prometheus.GaugeVec:
				if c == prometheus.Collector(m.InFlight) {
					m.InFlight = existing
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

// --- Retry hook ---

// retryHook receives a callback each time retry.Do fires a retry attempt.
// Single-client-per-process is the normal shape, so a global hook is
// acceptable here. nil means no-op.
var retryHook func(op, code string)

// SetRetryHook installs a callback invoked on every retry attempt with
// (op_label, grpc_status_code_string). Call once at client startup.
// Living here (not in pkg/client/io) avoids an import cycle: pkg/client/io
// already imports pkg/client/grpc, which would then need to import io to
// set the hook from the factory.
func SetRetryHook(fn func(op, code string)) { retryHook = fn }

// OnRetry fires the installed hook if one is set. Safe to call when no
// hook is installed.
func OnRetry(op, code string) {
	if retryHook != nil {
		retryHook(op, code)
	}
}

// --- Cache hooks ---
// Global callbacks, nil until wired by the binary. Safe to call without
// a hook installed (no-op). Wire up with SetCacheHitHook /
// SetCacheMissHook / SetCacheDedupeHitHook at client startup, alongside
// SetRetryHook.

var (
	cacheHitHook       func(tier, cacheType string)
	cacheMissHook      func(cacheType string)
	cacheDedupeHitHook func()
)

// SetCacheHitHook installs a callback invoked on a cache hit.
func SetCacheHitHook(fn func(tier, cacheType string)) { cacheHitHook = fn }

// SetCacheMissHook installs a callback invoked on a cache miss (both tiers missed).
func SetCacheMissHook(fn func(cacheType string)) { cacheMissHook = fn }

// SetCacheDedupeHitHook installs a callback invoked when WriteChunk dedupes a chunk.
func SetCacheDedupeHitHook(fn func()) { cacheDedupeHitHook = fn }

// CacheHit fires the hit hook for the given tier and sub-cache type.
// Safe to call when no hook is installed.
func CacheHit(tier, cacheType string) {
	if cacheHitHook != nil {
		cacheHitHook(tier, cacheType)
	}
}

// CacheMiss fires the miss hook for the given sub-cache type.
// Safe to call when no hook is installed.
func CacheMiss(cacheType string) {
	if cacheMissHook != nil {
		cacheMissHook(cacheType)
	}
}

// CacheDedupeHit fires the dedupe-hit hook.
// Safe to call when no hook is installed.
func CacheDedupeHit() {
	if cacheDedupeHitHook != nil {
		cacheDedupeHitHook()
	}
}
