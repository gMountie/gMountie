package metrics

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics is the set of custom client collectors. Construct via NewMetrics
// and register against a Registerer separately so tests use a fresh
// registry without polluting the global one.
type Metrics struct {
	RetryTotal *prometheus.CounterVec
	InFlight   *prometheus.GaugeVec
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
	}
}

// MustRegister registers all collectors with r. Panics on registration error.
func (m *Metrics) MustRegister(r prometheus.Registerer) {
	r.MustRegister(m.RetryTotal, m.InFlight)
}

// Register tolerates AlreadyRegisteredError by adopting the existing
// collector so increments still land in the registered series. Binaries
// that build a client more than once per process (or tests running with
// count>1) stay consistent.
func (m *Metrics) Register(r prometheus.Registerer) error {
	for _, c := range []prometheus.Collector{m.RetryTotal, m.InFlight} {
		if err := r.Register(c); err != nil {
			var ar prometheus.AlreadyRegisteredError
			if !errors.As(err, &ar) {
				return errors.Wrap(err, "register client metrics")
			}
			// Adopt the previously-registered collector so future
			// calls through m hit the same instance.
			switch existing := ar.ExistingCollector.(type) {
			case *prometheus.CounterVec:
				if c == prometheus.Collector(m.RetryTotal) {
					m.RetryTotal = existing
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
