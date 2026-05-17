package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
)

type ClientMetricsTestSuite struct {
	suite.Suite
	m *Metrics
}

func (s *ClientMetricsTestSuite) SetupTest() {
	s.m = NewMetrics()
	// Reset global hooks so tests are isolated from each other.
	SetCacheHitHook(nil)
	SetCacheMissHook(nil)
	SetCacheDedupeHitHook(nil)
	SetCacheRevalidationHook(nil)
	SetSubscribeEventReceivedHook(nil)
	SetSubscribeStreamStateHook(nil)
	SetCacheUnverifiedHook(nil)
}

func (s *ClientMetricsTestSuite) TestRetryInc() {
	s.m.RetryInc("Read", "Unavailable")
	s.m.RetryInc("Read", "Unavailable")
	s.Assert().Equal(2.0, testutil.ToFloat64(s.m.RetryTotal.WithLabelValues("Read", "Unavailable")))
}

func (s *ClientMetricsTestSuite) TestInFlightIncDec() {
	s.m.InFlightInc("Mkdir")
	s.m.InFlightInc("Mkdir")
	s.m.InFlightDec("Mkdir")
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.InFlight.WithLabelValues("Mkdir")))
}

func (s *ClientMetricsTestSuite) TestRetryHook_FiresWhenSet() {
	var seenOp, seenCode string
	SetRetryHook(func(op, code string) {
		seenOp = op
		seenCode = code
	})
	defer SetRetryHook(nil)

	OnRetry("Read", "Unavailable")
	s.Assert().Equal("Read", seenOp)
	s.Assert().Equal("Unavailable", seenCode)
}

func (s *ClientMetricsTestSuite) TestRetryHook_NoopWhenUnset() {
	SetRetryHook(nil)
	// must not panic
	OnRetry("Read", "Unavailable")
}

// --- Cache counter tests ---

func (s *ClientMetricsTestSuite) TestCacheHitInc_MemoryTier() {
	s.m.CacheHitInc("memory", "attr")
	s.m.CacheHitInc("memory", "attr")
	s.Assert().Equal(2.0, testutil.ToFloat64(s.m.CacheHits.WithLabelValues("memory", "attr")))
}

func (s *ClientMetricsTestSuite) TestCacheHitInc_DiskTier() {
	s.m.CacheHitInc("disk", "data")
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.CacheHits.WithLabelValues("disk", "data")))
	// Different tier must not pollute memory counter.
	s.Assert().Equal(0.0, testutil.ToFloat64(s.m.CacheHits.WithLabelValues("memory", "data")))
}

func (s *ClientMetricsTestSuite) TestCacheMissInc() {
	s.m.CacheMissInc("dir")
	s.m.CacheMissInc("dir")
	s.m.CacheMissInc("attr")
	s.Assert().Equal(2.0, testutil.ToFloat64(s.m.CacheMisses.WithLabelValues("dir")))
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.CacheMisses.WithLabelValues("attr")))
}

func (s *ClientMetricsTestSuite) TestCacheDedupeHitInc() {
	s.m.CacheDedupeHitInc()
	s.m.CacheDedupeHitInc()
	s.Assert().Equal(2.0, testutil.ToFloat64(s.m.CacheDedupeHits))
}

func (s *ClientMetricsTestSuite) TestCacheHitHook_FiresWhenSet() {
	var seenTier, seenType string
	SetCacheHitHook(func(tier, cacheType string) {
		seenTier = tier
		seenType = cacheType
	})
	CacheHit("memory", "attr")
	s.Assert().Equal("memory", seenTier)
	s.Assert().Equal("attr", seenType)
}

func (s *ClientMetricsTestSuite) TestCacheHitHook_NoopWhenUnset() {
	// must not panic
	CacheHit("memory", "attr")
}

func (s *ClientMetricsTestSuite) TestCacheMissHook_FiresWhenSet() {
	var seenType string
	SetCacheMissHook(func(cacheType string) { seenType = cacheType })
	CacheMiss("data")
	s.Assert().Equal("data", seenType)
}

func (s *ClientMetricsTestSuite) TestCacheMissHook_NoopWhenUnset() {
	// must not panic
	CacheMiss("data")
}

func (s *ClientMetricsTestSuite) TestCacheDedupeHitHook_FiresWhenSet() {
	fired := false
	SetCacheDedupeHitHook(func() { fired = true })
	CacheDedupeHit()
	s.Assert().True(fired)
}

func (s *ClientMetricsTestSuite) TestCacheDedupeHitHook_NoopWhenUnset() {
	// must not panic
	CacheDedupeHit()
}

func (s *ClientMetricsTestSuite) TestHookWiredToMetric_MemoryHit() {
	SetCacheHitHook(s.m.CacheHitInc)
	CacheHit("memory", "dir")
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.CacheHits.WithLabelValues("memory", "dir")))
}

func (s *ClientMetricsTestSuite) TestHookWiredToMetric_DiskHit() {
	SetCacheHitHook(s.m.CacheHitInc)
	CacheHit("disk", "attr")
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.CacheHits.WithLabelValues("disk", "attr")))
}

func (s *ClientMetricsTestSuite) TestHookWiredToMetric_Miss() {
	SetCacheMissHook(s.m.CacheMissInc)
	CacheMiss("dir")
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.CacheMisses.WithLabelValues("dir")))
}

func (s *ClientMetricsTestSuite) TestHookWiredToMetric_Dedupe() {
	SetCacheDedupeHitHook(s.m.CacheDedupeHitInc)
	CacheDedupeHit()
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.CacheDedupeHits))
}

// --- Revalidation hook tests ---

func (s *ClientMetricsTestSuite) TestCacheRevalidationInc() {
	s.m.CacheRevalidationInc("not_modified")
	s.m.CacheRevalidationInc("not_modified")
	s.m.CacheRevalidationInc("changed")
	s.Assert().Equal(2.0, testutil.ToFloat64(s.m.CacheRevalidations.WithLabelValues("not_modified")))
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.CacheRevalidations.WithLabelValues("changed")))
}

func (s *ClientMetricsTestSuite) TestCacheRevalidationHook_FiresWhenSet() {
	var seen string
	SetCacheRevalidationHook(func(result string) { seen = result })
	CacheRevalidation("enoent")
	s.Assert().Equal("enoent", seen)
}

func (s *ClientMetricsTestSuite) TestCacheRevalidationHook_NoopWhenUnset() {
	// must not panic
	CacheRevalidation("error")
}

func (s *ClientMetricsTestSuite) TestHookWiredToMetric_Revalidation() {
	SetCacheRevalidationHook(s.m.CacheRevalidationInc)
	CacheRevalidation("not_modified")
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.CacheRevalidations.WithLabelValues("not_modified")))
}

// --- SubscribeEventReceived hook tests ---

func (s *ClientMetricsTestSuite) TestSubscribeEventReceivedInc() {
	s.m.SubscribeEventReceivedInc("mutated")
	s.m.SubscribeEventReceivedInc("heartbeat")
	s.m.SubscribeEventReceivedInc("heartbeat")
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.SubscribeEventsReceived.WithLabelValues("mutated")))
	s.Assert().Equal(2.0, testutil.ToFloat64(s.m.SubscribeEventsReceived.WithLabelValues("heartbeat")))
}

func (s *ClientMetricsTestSuite) TestSubscribeEventReceivedHook_FiresWhenSet() {
	var seen string
	SetSubscribeEventReceivedHook(func(kind string) { seen = kind })
	SubscribeEventReceived("deleted")
	s.Assert().Equal("deleted", seen)
}

func (s *ClientMetricsTestSuite) TestSubscribeEventReceivedHook_NoopWhenUnset() {
	// must not panic
	SubscribeEventReceived("renamed")
}

func (s *ClientMetricsTestSuite) TestHookWiredToMetric_SubscribeEventReceived() {
	SetSubscribeEventReceivedHook(s.m.SubscribeEventReceivedInc)
	SubscribeEventReceived("mutated")
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.SubscribeEventsReceived.WithLabelValues("mutated")))
}

// --- SubscribeStreamState hook tests ---

func (s *ClientMetricsTestSuite) TestSubscribeStreamStateSet() {
	s.m.SubscribeStreamStateSet(true)
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.SubscribeStreamState))
	s.m.SubscribeStreamStateSet(false)
	s.Assert().Equal(0.0, testutil.ToFloat64(s.m.SubscribeStreamState))
}

func (s *ClientMetricsTestSuite) TestSubscribeStreamStateHook_FiresWhenSet() {
	var seen bool
	SetSubscribeStreamStateHook(func(up bool) { seen = up })
	SubscribeStreamStateChanged(true)
	s.Assert().True(seen)
}

func (s *ClientMetricsTestSuite) TestSubscribeStreamStateHook_NoopWhenUnset() {
	// must not panic
	SubscribeStreamStateChanged(false)
}

func (s *ClientMetricsTestSuite) TestHookWiredToMetric_SubscribeStreamState() {
	SetSubscribeStreamStateHook(s.m.SubscribeStreamStateSet)
	SubscribeStreamStateChanged(true)
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.SubscribeStreamState))
	SubscribeStreamStateChanged(false)
	s.Assert().Equal(0.0, testutil.ToFloat64(s.m.SubscribeStreamState))
}

// --- CacheUnverified hook tests ---

func (s *ClientMetricsTestSuite) TestCacheUnverifiedAdd() {
	s.m.CacheUnverifiedAdd(3.5)
	s.m.CacheUnverifiedAdd(1.5)
	s.Assert().Equal(5.0, testutil.ToFloat64(s.m.CacheUnverifiedDurationSecs))
}

func (s *ClientMetricsTestSuite) TestCacheUnverifiedHook_FiresWhenSet() {
	var seen float64
	SetCacheUnverifiedHook(func(seconds float64) { seen = seconds })
	CacheUnverifiedElapsed(7.25)
	s.Assert().Equal(7.25, seen)
}

func (s *ClientMetricsTestSuite) TestCacheUnverifiedHook_NoopWhenUnset() {
	// must not panic
	CacheUnverifiedElapsed(1.0)
}

func (s *ClientMetricsTestSuite) TestHookWiredToMetric_CacheUnverified() {
	SetCacheUnverifiedHook(s.m.CacheUnverifiedAdd)
	CacheUnverifiedElapsed(4.0)
	s.Assert().Equal(4.0, testutil.ToFloat64(s.m.CacheUnverifiedDurationSecs))
}

func TestClientMetricsTestSuite(t *testing.T) { suite.Run(t, new(ClientMetricsTestSuite)) }
