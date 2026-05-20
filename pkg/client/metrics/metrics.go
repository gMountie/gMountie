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

	// Subscribe / revalidation counters (Sub-spec D).
	CacheRevalidations            *prometheus.CounterVec
	SubscribeEventsReceived       *prometheus.CounterVec
	SubscribeStreamState          prometheus.Gauge
	CacheUnverifiedDurationSecs   prometheus.Counter
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
	}
}

// MustRegister registers all collectors with r. Panics on registration error.
func (m *Metrics) MustRegister(r prometheus.Registerer) {
	r.MustRegister(
		m.RetryTotal, m.InFlight, m.CacheHits, m.CacheMisses, m.CacheDedupeHits,
		m.CacheRevalidations, m.SubscribeEventsReceived, m.SubscribeStreamState,
		m.CacheUnverifiedDurationSecs,
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
				}
			case *prometheus.GaugeVec:
				if c == prometheus.Collector(m.InFlight) {
					m.InFlight = existing
				}
			// prometheus.Gauge embeds prometheus.Counter, so the Gauge
			// case must precede Counter — otherwise the Counter arm
			// silently catches Gauges.
			case prometheus.Gauge:
				if c == prometheus.Collector(m.SubscribeStreamState) {
					m.SubscribeStreamState = existing
				}
			case prometheus.Counter:
				switch c {
				case prometheus.Collector(m.CacheDedupeHits):
					m.CacheDedupeHits = existing
				case prometheus.Collector(m.CacheUnverifiedDurationSecs):
					m.CacheUnverifiedDurationSecs = existing
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

// --- Subscribe / revalidation hooks ---
// Global callbacks wired at client startup by pkg/client/grpc/factory.go.
// Safe to call when unset (no-op).

var (
	cacheRevalidationHook      func(result string)
	subscribeEventReceivedHook func(kind string)
	subscribeStreamStateHook   func(up bool)
	cacheUnverifiedHook        func(seconds float64)
)

// SetCacheRevalidationHook installs a callback invoked on each revalidation outcome.
func SetCacheRevalidationHook(fn func(result string)) { cacheRevalidationHook = fn }

// SetSubscribeEventReceivedHook installs a callback invoked for each Subscribe event handled.
func SetSubscribeEventReceivedHook(fn func(kind string)) { subscribeEventReceivedHook = fn }

// SetSubscribeStreamStateHook installs a callback invoked when the Subscribe stream
// transitions between up (true) and down/unverified (false).
func SetSubscribeStreamStateHook(fn func(up bool)) { subscribeStreamStateHook = fn }

// SetCacheUnverifiedHook installs a callback invoked with the number of seconds
// spent in unverified mode when the stream transitions back to verified.
func SetCacheUnverifiedHook(fn func(seconds float64)) { cacheUnverifiedHook = fn }

// CacheRevalidation fires the revalidation hook for the given result.
// Safe to call when no hook is installed.
func CacheRevalidation(result string) {
	if cacheRevalidationHook != nil {
		cacheRevalidationHook(result)
	}
}

// SubscribeEventReceived fires the subscribe-event-received hook for the given kind.
// Safe to call when no hook is installed.
func SubscribeEventReceived(kind string) {
	if subscribeEventReceivedHook != nil {
		subscribeEventReceivedHook(kind)
	}
}

// SubscribeStreamStateChanged fires the stream-state hook.
// Safe to call when no hook is installed.
func SubscribeStreamStateChanged(up bool) {
	if subscribeStreamStateHook != nil {
		subscribeStreamStateHook(up)
	}
}

// CacheUnverifiedElapsed fires the unverified-duration hook with elapsed seconds.
// Safe to call when no hook is installed.
func CacheUnverifiedElapsed(seconds float64) {
	if cacheUnverifiedHook != nil {
		cacheUnverifiedHook(seconds)
	}
}
