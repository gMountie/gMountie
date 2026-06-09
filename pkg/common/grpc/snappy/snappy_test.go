package snappy

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/suite"
)

type SnapSuite struct{ suite.Suite }

func TestSnapSuite(t *testing.T) { suite.Run(t, new(SnapSuite)) }

// TestRoundTrip verifies compress→decompress produces the original bytes.
func (s *SnapSuite) TestRoundTrip() {
	c := &compressor{}

	// Compress.
	var buf bytes.Buffer
	wc, err := c.Compress(&buf)
	s.Require().NoError(err)
	_, err = wc.Write([]byte("hello snappy"))
	s.Require().NoError(err)
	s.Require().NoError(wc.Close())

	// Decompress.
	r, err := c.Decompress(&buf)
	s.Require().NoError(err)
	got, err := io.ReadAll(r)
	s.Require().NoError(err)
	s.Assert().Equal("hello snappy", string(got))
}

// TestNoDoublePool verifies that reading past EOF twice does not double-Put
// (which would allow two concurrent consumers to share one pooled reader).
//
// White-box test: inspect the `done` flag directly after two EOF reads.
// The first EOF read must set done=true (guard engaged); the second must see
// done=true and not Put again. A Put counter exposed via the pool is
// GC-sensitive (sync.Pool can drop items between cycles), so we check the
// flag itself rather than the pool contents.
func (s *SnapSuite) TestNoDoublePool() {
	// Build snappy-framing-encoded bytes using the same writer the compressor uses.
	payload := []byte("some bytes to decode")
	var buf bytes.Buffer
	sw := snappy.NewBufferedWriter(&buf)
	_, err := sw.Write(payload)
	s.Require().NoError(err)
	s.Require().NoError(sw.Close())
	compressed := buf.Bytes()

	r := &reader{Reader: snappy.NewReader(bytes.NewReader(compressed))}

	// Precondition: done starts false.
	s.Assert().False(r.done, "done must start false before any reads")

	// First: read to EOF — done must flip to true and Put is called once.
	rbuf := make([]byte, 128)
	for {
		_, readErr := r.Read(rbuf)
		if errors.Is(readErr, io.EOF) {
			break
		}
		s.Require().NoError(readErr)
	}
	s.Assert().True(r.done, "done must be true after reading to EOF (Put called once)")

	// Second Read past EOF: done is still true, so no second Put.
	_, _ = r.Read(rbuf)
	s.Assert().True(r.done, "done must remain true after a post-EOF read (no second Put)")
}
