package io

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type CoalesceTestSuite struct {
	suite.Suite
}

// TestAppend_ContiguousSmallWritesAccumulate verifies the happy path: three
// small contiguous writes accumulate into a single buffer, none of them
// triggering a flush, and Drain returns the concatenation.
func (s *CoalesceTestSuite) TestAppend_ContiguousSmallWritesAccumulate() {
	c := NewWriteCoalescer(1 << 20)

	s.Require().Nil(c.Append([]byte("aaa"), 0))
	s.Require().Nil(c.Append([]byte("bbb"), 3))
	s.Require().Nil(c.Append([]byte("ccc"), 6))

	out := c.Drain()
	s.Require().NotNil(out)
	s.Assert().Equal(int64(0), out.Offset)
	s.Assert().Equal([]byte("aaabbbccc"), out.Data)
}

// TestAppend_DiscontiguousWriteForcesFlush verifies that an append at a
// non-contiguous offset returns the pending batch and starts a fresh buffer
// at the new offset.
func (s *CoalesceTestSuite) TestAppend_DiscontiguousWriteForcesFlush() {
	c := NewWriteCoalescer(1 << 20)

	s.Require().Nil(c.Append([]byte("aaa"), 0))

	// Discontiguous: gap before the new data.
	flushed := c.Append([]byte("zzz"), 100)
	s.Require().NotNil(flushed)
	s.Assert().Equal(int64(0), flushed.Offset)
	s.Assert().Equal([]byte("aaa"), flushed.Data)

	// The discontiguous write should be sitting in the buffer now.
	next := c.Drain()
	s.Require().NotNil(next)
	s.Assert().Equal(int64(100), next.Offset)
	s.Assert().Equal([]byte("zzz"), next.Data)
}

// TestAppend_ReachingThresholdForcesFlush verifies that an append that lands
// the buffer at exactly the threshold returns the full batch.
func (s *CoalesceTestSuite) TestAppend_ReachingThresholdForcesFlush() {
	c := NewWriteCoalescer(8)

	flushed := c.Append([]byte("12345678"), 0)
	s.Require().NotNil(flushed)
	s.Assert().Equal(int64(0), flushed.Offset)
	s.Assert().Equal([]byte("12345678"), flushed.Data)

	// Drain should now be a no-op.
	s.Assert().Nil(c.Drain())
}

// TestAppend_ExceedingThresholdInOneCallReturnsFullBatch verifies the caller
// can hand in more data than the threshold in a single call and receive the
// full batch back — we never truncate.
func (s *CoalesceTestSuite) TestAppend_ExceedingThresholdInOneCallReturnsFullBatch() {
	c := NewWriteCoalescer(4)

	payload := []byte("0123456789")
	flushed := c.Append(payload, 7)
	s.Require().NotNil(flushed)
	s.Assert().Equal(int64(7), flushed.Offset)
	s.Assert().Equal(payload, flushed.Data)

	s.Assert().Nil(c.Drain())
}

// TestDrain_EmptyReturnsNil verifies Drain on a pristine coalescer is a no-op.
func (s *CoalesceTestSuite) TestDrain_EmptyReturnsNil() {
	c := NewWriteCoalescer(1 << 20)
	s.Assert().Nil(c.Drain())
}

// TestDrain_DataIsCopiedNotAliased verifies the slice returned from Drain is
// not aliased with the internal buffer — the caller is free to mutate it
// without corrupting future Append output.
func (s *CoalesceTestSuite) TestDrain_DataIsCopiedNotAliased() {
	c := NewWriteCoalescer(1 << 20)

	s.Require().Nil(c.Append([]byte("hello"), 0))
	drained := c.Drain()
	s.Require().NotNil(drained)

	// Scribble on the returned slice.
	for i := range drained.Data {
		drained.Data[i] = 'X'
	}

	// A fresh append should land at offset 0 with its own data intact.
	s.Require().Nil(c.Append([]byte("world"), 0))
	next := c.Drain()
	s.Require().NotNil(next)
	s.Assert().Equal([]byte("world"), next.Data,
		"internal buffer must not be aliased with a previously-drained slice")
}

func TestCoalesceSuite(t *testing.T) {
	suite.Run(t, new(CoalesceTestSuite))
}
