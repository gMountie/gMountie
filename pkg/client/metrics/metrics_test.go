package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
)

type ClientMetricsTestSuite struct {
	suite.Suite
	m *Metrics
}

func (s *ClientMetricsTestSuite) SetupTest() {
	s.m = NewMetrics()
	// Reset the global instance registry so tests are isolated.
	instancesMu.Lock()
	instances = nil
	instancesMu.Unlock()
}

func (s *ClientMetricsTestSuite) TearDownTest() {
	// Clean up: remove any instance registered by this test.
	instancesMu.Lock()
	instances = nil
	instancesMu.Unlock()
}

func (s *ClientMetricsTestSuite) TestRetryInc() {
	s.m.RetryInc("Read", "Unavailable")
	s.m.RetryInc("Read", "Unavailable")
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.RetryTotal.WithLabelValues("Read", "Unavailable"))))
}

func (s *ClientMetricsTestSuite) TestInFlightIncDec() {
	s.m.InFlightInc("Mkdir")
	s.m.InFlightInc("Mkdir")
	s.m.InFlightDec("Mkdir")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.InFlight.WithLabelValues("Mkdir"))))
}

func (s *ClientMetricsTestSuite) TestOnRetry_FiresWhenRegistered() {
	RegisterInstance(s.m)
	OnRetry("Read", "Unavailable")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.RetryTotal.WithLabelValues("Read", "Unavailable"))))
}

func (s *ClientMetricsTestSuite) TestOnRetry_NoopWhenEmpty() {
	// must not panic
	OnRetry("Read", "Unavailable")
}

// --- Cache counter tests ---

func (s *ClientMetricsTestSuite) TestCacheHitInc_MemoryTier() {
	s.m.CacheHitInc("memory", "attr")
	s.m.CacheHitInc("memory", "attr")
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.CacheHits.WithLabelValues("memory", "attr"))))
}

func (s *ClientMetricsTestSuite) TestCacheHitInc_DiskTier() {
	s.m.CacheHitInc("disk", "data")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.CacheHits.WithLabelValues("disk", "data"))))
	// Different tier must not pollute memory counter.
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.CacheHits.WithLabelValues("memory", "data"))))
}

func (s *ClientMetricsTestSuite) TestCacheMissInc() {
	s.m.CacheMissInc("dir")
	s.m.CacheMissInc("dir")
	s.m.CacheMissInc("attr")
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.CacheMisses.WithLabelValues("dir"))))
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.CacheMisses.WithLabelValues("attr"))))
}

func (s *ClientMetricsTestSuite) TestCacheDedupeHitInc() {
	s.m.CacheDedupeHitInc()
	s.m.CacheDedupeHitInc()
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.CacheDedupeHits)))
}

func (s *ClientMetricsTestSuite) TestCacheHit_DispatchesToRegistered() {
	RegisterInstance(s.m)
	CacheHit("memory", "attr")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.CacheHits.WithLabelValues("memory", "attr"))))
}

func (s *ClientMetricsTestSuite) TestCacheHit_NoopWhenEmpty() {
	// must not panic
	CacheHit("memory", "attr")
}

func (s *ClientMetricsTestSuite) TestCacheMiss_DispatchesToRegistered() {
	RegisterInstance(s.m)
	CacheMiss("data")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.CacheMisses.WithLabelValues("data"))))
}

func (s *ClientMetricsTestSuite) TestCacheMiss_NoopWhenEmpty() {
	// must not panic
	CacheMiss("data")
}

func (s *ClientMetricsTestSuite) TestCacheDedupeHit_DispatchesToRegistered() {
	RegisterInstance(s.m)
	CacheDedupeHit()
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.CacheDedupeHits)))
}

func (s *ClientMetricsTestSuite) TestCacheDedupeHit_NoopWhenEmpty() {
	// must not panic
	CacheDedupeHit()
}

// --- Multi-instance tests: two clients must each see their own counts ---

func (s *ClientMetricsTestSuite) TestMultiInstance_NoGlobalCrossWire() {
	reg := prometheus.NewRegistry()
	m2 := NewMetrics()
	s.Require().NoError(m2.Register(reg))

	RegisterInstance(s.m)
	RegisterInstance(m2)

	CacheHit("memory", "dir")

	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.CacheHits.WithLabelValues("memory", "dir"))))
	s.Assert().Equal(1, int(testutil.ToFloat64(m2.CacheHits.WithLabelValues("memory", "dir"))))
}

// --- Revalidation tests ---

func (s *ClientMetricsTestSuite) TestCacheRevalidationInc() {
	s.m.CacheRevalidationInc("not_modified")
	s.m.CacheRevalidationInc("not_modified")
	s.m.CacheRevalidationInc("changed")
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.CacheRevalidations.WithLabelValues("not_modified"))))
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.CacheRevalidations.WithLabelValues("changed"))))
}

func (s *ClientMetricsTestSuite) TestCacheRevalidation_DispatchesToRegistered() {
	RegisterInstance(s.m)
	CacheRevalidation("enoent")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.CacheRevalidations.WithLabelValues("enoent"))))
}

func (s *ClientMetricsTestSuite) TestCacheRevalidation_NoopWhenEmpty() {
	// must not panic
	CacheRevalidation("error")
}

// --- SubscribeEventReceived tests ---

func (s *ClientMetricsTestSuite) TestSubscribeEventReceivedInc() {
	s.m.SubscribeEventReceivedInc("mutated")
	s.m.SubscribeEventReceivedInc("heartbeat")
	s.m.SubscribeEventReceivedInc("heartbeat")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.SubscribeEventsReceived.WithLabelValues("mutated"))))
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.SubscribeEventsReceived.WithLabelValues("heartbeat"))))
}

func (s *ClientMetricsTestSuite) TestSubscribeEventReceived_DispatchesToRegistered() {
	RegisterInstance(s.m)
	SubscribeEventReceived("deleted")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.SubscribeEventsReceived.WithLabelValues("deleted"))))
}

func (s *ClientMetricsTestSuite) TestSubscribeEventReceived_NoopWhenEmpty() {
	// must not panic
	SubscribeEventReceived("renamed")
}

// --- SubscribeStreamState tests ---

func (s *ClientMetricsTestSuite) TestSubscribeStreamStateSet() {
	s.m.SubscribeStreamStateSet(true)
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.SubscribeStreamState)))
	s.m.SubscribeStreamStateSet(false)
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.SubscribeStreamState)))
}

func (s *ClientMetricsTestSuite) TestSubscribeStreamStateChanged_DispatchesToRegistered() {
	RegisterInstance(s.m)
	SubscribeStreamStateChanged(true)
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.SubscribeStreamState)))
	SubscribeStreamStateChanged(false)
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.SubscribeStreamState)))
}

func (s *ClientMetricsTestSuite) TestSubscribeStreamStateChanged_NoopWhenEmpty() {
	// must not panic
	SubscribeStreamStateChanged(false)
}

// --- CacheUnverified tests ---

func (s *ClientMetricsTestSuite) TestCacheUnverifiedAdd() {
	s.m.CacheUnverifiedAdd(3.5)
	s.m.CacheUnverifiedAdd(1.5)
	s.Assert().Equal(5, int(testutil.ToFloat64(s.m.CacheUnverifiedDurationSecs)))
}

func (s *ClientMetricsTestSuite) TestCacheUnverifiedElapsed_DispatchesToRegistered() {
	RegisterInstance(s.m)
	CacheUnverifiedElapsed(4.0)
	s.Assert().Equal(4, int(testutil.ToFloat64(s.m.CacheUnverifiedDurationSecs)))
}

func (s *ClientMetricsTestSuite) TestCacheUnverifiedElapsed_NoopWhenEmpty() {
	// must not panic
	CacheUnverifiedElapsed(1.0)
}

func (s *ClientMetricsTestSuite) TestRegisterInstance_Idempotent() {
	RegisterInstance(s.m)
	RegisterInstance(s.m)
	RegisterInstance(s.m)

	instancesMu.RLock()
	count := len(instances)
	instancesMu.RUnlock()
	s.Assert().Equal(1, count, "RegisterInstance must deduplicate")
}

// TestOnRetry_SharedCollectorsCountOnce is the regression test for the
// double-count bug: the standard mount flow constructs two clients, whose
// Metrics adopt the SAME underlying collectors via Register's
// AlreadyRegisteredError handling. Fan-out must increment the shared
// CounterVec once per event, not once per registered wrapper.
func (s *ClientMetricsTestSuite) TestOnRetry_SharedCollectorsCountOnce() {
	reg := prometheus.NewRegistry()
	s.Require().NoError(s.m.Register(reg))
	m2 := NewMetrics()
	s.Require().NoError(m2.Register(reg)) // adopts s.m's collectors
	s.Require().True(sameCollectors(s.m, m2), "m2 must have adopted s.m's collectors")

	RegisterInstance(s.m)
	RegisterInstance(m2)

	OnRetry("Read", "Unavailable")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.RetryTotal.WithLabelValues("Read", "Unavailable"))),
		"one retry must count once, regardless of how many clients share the collectors")
}

// TestUnregisterInstance_RefCounted verifies the dispatcher lifecycle that
// ClientImpl.Close relies on: closing one of two clients sharing a collector
// set keeps the fan-out alive for the survivor; closing the last one stops it.
func (s *ClientMetricsTestSuite) TestUnregisterInstance_RefCounted() {
	reg := prometheus.NewRegistry()
	s.Require().NoError(s.m.Register(reg))
	m2 := NewMetrics()
	s.Require().NoError(m2.Register(reg))

	RegisterInstance(s.m)
	RegisterInstance(m2)

	// First client closes: the shared entry must survive for the second.
	UnregisterInstance(s.m)
	OnRetry("Read", "Unavailable")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.RetryTotal.WithLabelValues("Read", "Unavailable"))))

	// Last client closes: fan-out stops.
	UnregisterInstance(m2)
	OnRetry("Read", "Unavailable")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.RetryTotal.WithLabelValues("Read", "Unavailable"))),
		"a closed client set must no longer receive fan-out")
}

func TestClientMetricsTestSuite(t *testing.T) { suite.Run(t, new(ClientMetricsTestSuite)) }
