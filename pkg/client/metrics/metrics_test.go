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

// --- Revalidation tests ---

func (s *ClientMetricsTestSuite) TestCacheRevalidationInc() {
	s.m.CacheRevalidationInc("not_modified")
	s.m.CacheRevalidationInc("not_modified")
	s.m.CacheRevalidationInc("changed")
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.CacheRevalidations.WithLabelValues("not_modified"))))
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.CacheRevalidations.WithLabelValues("changed"))))
}

// --- SubscribeEventReceived tests ---

func (s *ClientMetricsTestSuite) TestSubscribeEventReceivedInc() {
	s.m.SubscribeEventReceivedInc("mutated")
	s.m.SubscribeEventReceivedInc("heartbeat")
	s.m.SubscribeEventReceivedInc("heartbeat")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.SubscribeEventsReceived.WithLabelValues("mutated"))))
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.SubscribeEventsReceived.WithLabelValues("heartbeat"))))
}

// --- SubscribeStreamState tests ---

func (s *ClientMetricsTestSuite) TestSubscribeStreamStateSet() {
	s.m.SubscribeStreamStateSet(true)
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.SubscribeStreamState)))
	s.m.SubscribeStreamStateSet(false)
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.SubscribeStreamState)))
}

// --- CacheUnverified tests ---

func (s *ClientMetricsTestSuite) TestCacheUnverifiedAdd() {
	s.m.CacheUnverifiedAdd(3.5)
	s.m.CacheUnverifiedAdd(1.5)
	s.Assert().Equal(5, int(testutil.ToFloat64(s.m.CacheUnverifiedDurationSecs)))
}

// --- Persist GC / disk-accounting counters (blind spot #4) ---

func (s *ClientMetricsTestSuite) TestChunkUnlinkedInc() {
	s.m.ChunkUnlinkedInc("refcount_zero")
	s.m.ChunkUnlinkedInc("refcount_zero")
	s.m.ChunkUnlinkedInc("ghost")
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.ChunksUnlinked.WithLabelValues("refcount_zero"))))
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.ChunksUnlinked.WithLabelValues("ghost"))))
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.ChunksUnlinked.WithLabelValues("orphan"))))
}

func (s *ClientMetricsTestSuite) TestGhostEntryDeletedInc() {
	s.m.GhostEntryDeletedInc()
	s.m.GhostEntryDeletedInc()
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.GhostEntriesDeleted)))
}

func (s *ClientMetricsTestSuite) TestRefcountUnderflowInc() {
	s.m.RefcountUnderflowInc()
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.RefcountUnderflows)))
}

func (s *ClientMetricsTestSuite) TestOrphanReclaimedInc() {
	s.m.OrphanReclaimedInc()
	s.m.OrphanReclaimedInc()
	s.m.OrphanReclaimedInc()
	s.Assert().Equal(3, int(testutil.ToFloat64(s.m.OrphansReclaimed)))
}

func (s *ClientMetricsTestSuite) TestTmpReclaimedInc() {
	s.m.TmpReclaimedInc()
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.TmpReclaimed)))
}

func (s *ClientMetricsTestSuite) TestBudgetEvictionInc() {
	s.m.BudgetEvictionInc()
	s.m.BudgetEvictionInc()
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.BudgetEvictions)))
}

func (s *ClientMetricsTestSuite) TestDiskBytesUsedSet() {
	s.m.DiskBytesUsedSet(4096)
	s.Assert().Equal(4096, int(testutil.ToFloat64(s.m.DiskBytesUsed)))
	s.m.DiskBytesUsedSet(0)
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.DiskBytesUsed)))
}

// TestGCMetrics_RegisterAdoptsGaugeBeforeCounter is the regression guard for
// the adopt-switch ordering: a plain prometheus.Gauge also satisfies the
// prometheus.Counter interface, so the new DiskBytesUsed gauge must be adopted
// by the Gauge arm (which precedes Counter). Registering twice against one
// registry makes the second set adopt the first's collectors; if the ordering
// were wrong, the gauge would be miscast and increments would not land.
func (s *ClientMetricsTestSuite) TestGCMetrics_RegisterAdoptsGaugeBeforeCounter() {
	reg := prometheus.NewRegistry()
	s.Require().NoError(s.m.Register(reg))
	m2 := NewMetrics()
	s.Require().NoError(m2.Register(reg)) // adopts s.m's collectors

	// m2 must have adopted the SAME gauge instance as s.m.
	m2.DiskBytesUsedSet(8192)
	s.Assert().Equal(8192, int(testutil.ToFloat64(s.m.DiskBytesUsed)),
		"adopted gauge must be the shared instance (Gauge arm before Counter)")

	// And a plain Counter still adopts correctly.
	m2.GhostEntryDeletedInc()
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.GhostEntriesDeleted)))

	// The labelled vec adopts too.
	m2.ChunkUnlinkedInc("budget")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.ChunksUnlinked.WithLabelValues("budget"))))
}

func TestClientMetricsTestSuite(t *testing.T) { suite.Run(t, new(ClientMetricsTestSuite)) }
