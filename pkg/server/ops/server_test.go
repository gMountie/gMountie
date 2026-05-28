package ops

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ServerTestSuite struct{ suite.Suite }

// muxOf reaches into a freshly-built Server, pulls its Handler, and
// runs requests against it via httptest. Avoids actually opening a
// listener while still exercising real routing.
func muxOf(s *Server) http.Handler { return s.server.Handler }

func (s *ServerTestSuite) TestPprofDisabledByDefault() {
	srv := NewServer(":0", stubReadiness{}, false, nil)
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		muxOf(srv).ServeHTTP(rr, req)
		s.Assert().Equal(http.StatusNotFound, rr.Code,
			"pprof leaked at %s with enablePprof=false", path)
	}
}

func (s *ServerTestSuite) TestPprofEnabledServesIndex() {
	srv := NewServer(":0", stubReadiness{}, true, nil)
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rr := httptest.NewRecorder()
	muxOf(srv).ServeHTTP(rr, req)
	s.Assert().Equal(http.StatusOK, rr.Code)
	// pprof.Index renders an HTML page listing the profile names —
	// "goroutine" is the most stable substring across Go versions.
	s.Assert().Contains(rr.Body.String(), "goroutine")
}

func (s *ServerTestSuite) TestPprofEnabledLeavesCoreRoutesIntact() {
	srv := NewServer(":0", stubReadiness{}, true, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	muxOf(srv).ServeHTTP(rr, req)
	s.Assert().Equal(http.StatusOK, rr.Code)
}

func TestServerTestSuite(t *testing.T) { suite.Run(t, new(ServerTestSuite)) }
