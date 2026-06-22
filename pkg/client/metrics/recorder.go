package metrics

// Recorder is the per-client metrics sink injected into the layers (cache,
// the metrics observer) and used by the transport retry path. *Metrics is the
// default Prometheus implementation; OTel/audit backends implement Recorder
// later without touching the layers. Defining it here (a leaf package with no
// io/grpc deps) removes the old import cycle that the package-global dispatcher
// existed to dodge.
type Recorder interface {
	RetryInc(op, code string)
	CacheHitInc(tier, cacheType string)
	CacheMissInc(cacheType string)
	CacheDedupeHitInc()
	CachePersistDroppedInc()
	CacheRevalidationInc(result string)
	SubscribeEventReceivedInc(kind string)
	SubscribeStreamStateSet(up bool)
	CacheUnverifiedAdd(seconds float64)
	InFlightInc(op string)
	InFlightDec(op string)
	ObserveOp(op string, seconds float64, code string)
}

var _ Recorder = (*Metrics)(nil)
