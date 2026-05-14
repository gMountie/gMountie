package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics is the set of custom server collectors. Construct via NewMetrics
// and register against a Registerer separately so tests use a fresh
// registry without polluting the global one.
type Metrics struct {
	OpenFiles       *prometheus.GaugeVec
	Bytes           *prometheus.CounterVec
	RpcErrors       *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	SessionsActive  prometheus.Gauge
}

// NewMetrics constructs the set of server collectors. They are NOT
// registered — call MustRegister or Register on a chosen Registerer.
func NewMetrics() *Metrics {
	return &Metrics{
		OpenFiles: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gmountie_server_open_files",
			Help: "Number of file descriptors currently open on the server, per volume and session.",
		}, []string{"volume", "session"}),
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
	}
}

// MustRegister registers all collectors with r. Panics on registration error.
func (m *Metrics) MustRegister(r prometheus.Registerer) {
	r.MustRegister(m.OpenFiles, m.Bytes, m.RpcErrors, m.RequestDuration, m.SessionsActive)
}

// Register registers all collectors with r. Already-registered collectors
// are tolerated (useful when the same default registerer is reused across
// `go test -count=N` runs).
func (m *Metrics) Register(r prometheus.Registerer) error {
	for _, c := range []prometheus.Collector{m.OpenFiles, m.Bytes, m.RpcErrors, m.RequestDuration, m.SessionsActive} {
		if err := r.Register(c); err != nil {
			are, ok := err.(prometheus.AlreadyRegisteredError)
			if !ok {
				return err
			}
			// Reuse the previously-registered collector so future calls
			// hit the same instance.
			switch existing := are.ExistingCollector.(type) {
			case *prometheus.GaugeVec:
				if c == prometheus.Collector(m.OpenFiles) {
					m.OpenFiles = existing
				}
			case *prometheus.CounterVec:
				if c == prometheus.Collector(m.Bytes) {
					m.Bytes = existing
				} else if c == prometheus.Collector(m.RpcErrors) {
					m.RpcErrors = existing
				}
			case *prometheus.HistogramVec:
				m.RequestDuration = existing
			case prometheus.Gauge:
				m.SessionsActive = existing
			}
		}
	}
	return nil
}

func (m *Metrics) OpenFilesInc(volume, session string) {
	m.OpenFiles.WithLabelValues(volume, session).Inc()
}

func (m *Metrics) OpenFilesDec(volume, session string) {
	m.OpenFiles.WithLabelValues(volume, session).Dec()
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
