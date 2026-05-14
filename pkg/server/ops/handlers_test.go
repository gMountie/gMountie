package ops

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gmountie/pkg"

	"github.com/stretchr/testify/suite"
)

type stubReadiness struct{ err error }

func (s stubReadiness) Ready(_ context.Context) error { return s.err }

type HandlersTestSuite struct{ suite.Suite }

func (s *HandlersTestSuite) TestLivenessAlways200() {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	LivenessHandler().ServeHTTP(rr, req)
	s.Assert().Equal(http.StatusOK, rr.Code)
	s.Assert().Equal("ok", rr.Body.String())
}

func (s *HandlersTestSuite) TestReadinessReady() {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	ReadinessHandler(stubReadiness{}).ServeHTTP(rr, req)
	s.Assert().Equal(http.StatusOK, rr.Code)
}

func (s *HandlersTestSuite) TestReadinessNotReady() {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	ReadinessHandler(stubReadiness{err: errors.New("volume gone")}).ServeHTTP(rr, req)
	s.Assert().Equal(http.StatusServiceUnavailable, rr.Code)
	s.Assert().Contains(rr.Body.String(), "volume gone")
}

func (s *HandlersTestSuite) TestVersionReturnsBuildInfo() {
	req := httptest.NewRequest(http.MethodGet, "/version", nil)
	rr := httptest.NewRecorder()
	VersionHandler().ServeHTTP(rr, req)
	s.Require().Equal(http.StatusOK, rr.Code)
	s.Assert().Contains(rr.Header().Get("Content-Type"), "application/json")
	var got pkg.BuildInfo
	s.Require().NoError(json.NewDecoder(strings.NewReader(rr.Body.String())).Decode(&got))
	s.Assert().NotEmpty(got.Version)
}

func TestHandlersTestSuite(t *testing.T) { suite.Run(t, new(HandlersTestSuite)) }
