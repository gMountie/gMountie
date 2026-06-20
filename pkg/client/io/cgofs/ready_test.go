//go:build darwin || cgofuse

package cgofs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ReadySuite struct{ suite.Suite }

func TestReadySuite(t *testing.T) { suite.Run(t, new(ReadySuite)) }

func (s *ReadySuite) TestInitClosesReady() {
	fs := New(&fakeBackend{}, nil)
	select {
	case <-fs.Ready():
		s.Fail("ready closed before Init")
	default:
	}
	fs.Init()
	select {
	case <-fs.Ready():
	case <-time.After(time.Second):
		s.Fail("ready not closed after Init")
	}
}

func (s *ReadySuite) TestDestroyClosesDone() {
	fs := New(&fakeBackend{}, nil)
	fs.Destroy()
	select {
	case <-fs.Done():
	case <-time.After(time.Second):
		s.Fail("done not closed after Destroy")
	}
}
