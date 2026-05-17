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

func TestClientMetricsTestSuite(t *testing.T) { suite.Run(t, new(ClientMetricsTestSuite)) }
