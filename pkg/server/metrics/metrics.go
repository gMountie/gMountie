package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics is the set of custom server collectors. Construct via NewMetrics
// and register against a Registerer separately so tests use a fresh
// registry without polluting the global one.
type Metrics struct {
	OpenFiles       *prometheus.GaugeVec
	Bytes           *prometheus.CounterVec
	RpcErrors       *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	SessionsActive  prometheus.Gauge
	// SessionsReaped counts sessions removed, by reason (grace_expired,
	// revoked, shutdown), so operators can tell normal grace reaps from
	// revocation-driven or shutdown reaps (OB-L1).
	SessionsReaped *prometheus.CounterVec
	// AuthFailures counts denied authentications, per method and reason
	// (authorize_error, nil_user, revoked). The AuthInterceptor sits ahead of
	// the finish-call logger and the metrics interceptors, so without this a
	// denial emits nothing at all (OB-H1).
	AuthFailures *prometheus.CounterVec

	// TLSReloadFailures counts failed server-cert reload attempts (stat blip,
	// cert/key mismatch, unparsable PEM). The Reloader fails open — it keeps
	// serving the last good pair — so without this counter a stuck-stale cert
	// is invisible (OB-M2).
	TLSReloadFailures prometheus.Counter
	// TLSLastReloadUnixtime is the wall-clock time of the last SUCCESSFUL cert
	// reload. Alert on staleness to catch a reloader that's failing open.
	TLSLastReloadUnixtime prometheus.Gauge

	// Subscribe counters (Sub-spec D).
	SubscribeEventsEmitted *prometheus.CounterVec
	SubscribeSubscribers   *prometheus.GaugeVec
	SubscribeDroppedSlow   *prometheus.CounterVec
}

// NewMetrics constructs the set of server collectors. They are NOT
// registered — call MustRegister or Register on a chosen Registerer.
func NewMetrics() *Metrics {
	return &Metrics{
		// Per-volume only — deliberately NO per-session label: session ids are
		// bearer tokens (must never appear in /metrics, see common.FingerprintID)
		// and even fingerprinted they'd leak one label series per session for
		// the server's lifetime. Per-session fd detail belongs in logs.
		OpenFiles: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gmountie_server_open_files",
			Help: "Number of file descriptors currently open on the server, per volume.",
		}, []string{"volume"}),
		Bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_server_bytes_total",
			Help: "Total bytes transferred per volume and direction (in=write, out=read).",
		}, []string{"volume", "direction"}),
		RpcErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_server_rpc_errors_total",
			Help: "Count of non-OK gRPC RPCs per volume, op, and grpc status code.",
		}, []string{"volume", "op", "code"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gmountie_server_request_duration_seconds",
			Help:    "Per-RPC handler duration in seconds, per volume and op.",
			Buckets: prometheus.DefBuckets,
		}, []string{"volume", "op"}),
		SessionsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gmountie_server_sessions_active",
			Help: "Number of active sessions (created and not yet reaped).",
		}),
		SessionsReaped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_server_sessions_reaped_total",
			Help: "Count of sessions reaped, per reason (grace_expired, revoked, shutdown).",
		}, []string{"reason"}),
		AuthFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_server_auth_failures_total",
			Help: "Count of denied authentications, per method and reason.",
		}, []string{"method", "reason"}),
		TLSReloadFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gmountie_server_tls_reload_failures_total",
			Help: "Count of failed server-cert reload attempts (reloader fails open, serving the last good pair).",
		}),
		TLSLastReloadUnixtime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gmountie_server_tls_last_reload_unixtime_seconds",
			Help: "Unix time of the last successful server-cert reload.",
		}),
		SubscribeEventsEmitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_subscribe_events_emitted_total",
			Help: "Subscribe events emitted to subscribers per kind.",
		}, []string{"kind"}),
		SubscribeSubscribers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gmountie_subscribe_subscribers",
			Help: "Number of active Subscribe stream clients per volume.",
		}, []string{"volume"}),
		SubscribeDroppedSlow: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gmountie_subscribe_dropped_slow_total",
			Help: "Subscribe events dropped due to slow consumers per volume.",
		}, []string{"volume"}),
	}
}

// MustRegister registers all collectors with r. Panics on registration error.
func (m *Metrics) MustRegister(r prometheus.Registerer) {
	r.MustRegister(
		m.OpenFiles, m.Bytes, m.RpcErrors, m.RequestDuration, m.SessionsActive,
		m.SessionsReaped, m.AuthFailures, m.TLSReloadFailures, m.TLSLastReloadUnixtime,
		m.SubscribeEventsEmitted, m.SubscribeSubscribers, m.SubscribeDroppedSlow,
	)
}

// Register registers all collectors with r. Already-registered collectors
// are tolerated (useful when the same default registerer is reused across
// `go test -count=N` runs).
func (m *Metrics) Register(r prometheus.Registerer) error {
	all := []prometheus.Collector{
		m.OpenFiles, m.Bytes, m.RpcErrors, m.RequestDuration, m.SessionsActive,
		m.SessionsReaped, m.AuthFailures, m.TLSReloadFailures, m.TLSLastReloadUnixtime,
		m.SubscribeEventsEmitted, m.SubscribeSubscribers, m.SubscribeDroppedSlow,
	}
	for _, c := range all {
		if err := r.Register(c); err != nil {
			var are prometheus.AlreadyRegisteredError
			ok := errors.As(err, &are)
			if !ok {
				return err
			}
			// Reuse the previously-registered collector so future calls
			// hit the same instance.
			switch existing := are.ExistingCollector.(type) {
			case *prometheus.GaugeVec:
				switch c {
				case prometheus.Collector(m.OpenFiles):
					m.OpenFiles = existing
				case prometheus.Collector(m.SubscribeSubscribers):
					m.SubscribeSubscribers = existing
				}
			case *prometheus.CounterVec:
				switch c {
				case prometheus.Collector(m.Bytes):
					m.Bytes = existing
				case prometheus.Collector(m.RpcErrors):
					m.RpcErrors = existing
				case prometheus.Collector(m.SubscribeEventsEmitted):
					m.SubscribeEventsEmitted = existing
				case prometheus.Collector(m.SubscribeDroppedSlow):
					m.SubscribeDroppedSlow = existing
				case prometheus.Collector(m.SessionsReaped):
					m.SessionsReaped = existing
				case prometheus.Collector(m.AuthFailures):
					m.AuthFailures = existing
				}
			case *prometheus.HistogramVec:
				m.RequestDuration = existing
			case prometheus.Counter:
				switch c {
				case prometheus.Collector(m.TLSReloadFailures):
					m.TLSReloadFailures = existing
				}
			case prometheus.Gauge:
				// Two plain gauges exist — disambiguate, or TLSLastReloadUnixtime
				// would be mis-adopted onto SessionsActive under a shared
				// registerer (go test -count=N).
				switch c {
				case prometheus.Collector(m.SessionsActive):
					m.SessionsActive = existing
				case prometheus.Collector(m.TLSLastReloadUnixtime):
					m.TLSLastReloadUnixtime = existing
				}
			}
		}
	}
	return nil
}

// OpenFilesInc/OpenFilesDec are called from the session layer (RegisterFile /
// ReleaseFile / ReleaseAll), NOT from the RPC handlers — that is what keeps
// the gauge honest across session reaps (grace expiry, revocation, shutdown)
// and makes a retried Release idempotent (only an actual fd removal
// decrements).
func (m *Metrics) OpenFilesInc(volume string) {
	m.OpenFiles.WithLabelValues(volume).Inc()
}

func (m *Metrics) OpenFilesDec(volume string) {
	m.OpenFiles.WithLabelValues(volume).Dec()
}

func (m *Metrics) BytesAdd(volume, direction string, n float64) {
	m.Bytes.WithLabelValues(volume, direction).Add(n)
}

func (m *Metrics) RpcErrorInc(volume, op, code string) {
	m.RpcErrors.WithLabelValues(volume, op, code).Inc()
}

func (m *Metrics) RequestDurationObserve(volume, op string, seconds float64) {
	m.RequestDuration.WithLabelValues(volume, op).Observe(seconds)
}

func (m *Metrics) SessionsActiveInc() { m.SessionsActive.Inc() }
func (m *Metrics) SessionsActiveDec() { m.SessionsActive.Dec() }

// SessionsReapedInc bumps the reaped-sessions counter for the given reason.
func (m *Metrics) SessionsReapedInc(reason string) {
	m.SessionsReaped.WithLabelValues(reason).Inc()
}

// AuthFailureInc bumps the auth-failure counter for the given method and reason.
func (m *Metrics) AuthFailureInc(method, reason string) {
	m.AuthFailures.WithLabelValues(method, reason).Inc()
}

// TLSReloadFailureInc bumps the cert-reload-failure counter.
func (m *Metrics) TLSReloadFailureInc() { m.TLSReloadFailures.Inc() }

// TLSReloadSucceeded stamps the last-successful-reload gauge to now.
func (m *Metrics) TLSReloadSucceeded() { m.TLSLastReloadUnixtime.SetToCurrentTime() }

// SubscribeEventEmittedInc bumps the emitted-events counter for the given kind.
func (m *Metrics) SubscribeEventEmittedInc(kind string) {
	m.SubscribeEventsEmitted.WithLabelValues(kind).Inc()
}

// SubscribeSubscribersInc increments the active-subscribers gauge for volume.
func (m *Metrics) SubscribeSubscribersInc(volume string) {
	m.SubscribeSubscribers.WithLabelValues(volume).Inc()
}

// SubscribeSubscribersDec decrements the active-subscribers gauge for volume.
func (m *Metrics) SubscribeSubscribersDec(volume string) {
	m.SubscribeSubscribers.WithLabelValues(volume).Dec()
}

// SubscribeDroppedSlowInc bumps the dropped-slow counter for volume.
func (m *Metrics) SubscribeDroppedSlowInc(volume string) {
	m.SubscribeDroppedSlow.WithLabelValues(volume).Inc()
}
