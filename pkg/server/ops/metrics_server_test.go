package ops

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"
)

type MetricsServerTestSuite struct{ suite.Suite }

func TestMetricsServerTestSuite(t *testing.T) { suite.Run(t, new(MetricsServerTestSuite)) }

func (s *MetricsServerTestSuite) get(path string) *httptest.ResponseRecorder {
	srv := NewMetricsServer(":0")
	rec := httptest.NewRecorder()
	muxOf(srv).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func (s *MetricsServerTestSuite) TestServesMetricsWithoutAuth() {
	rec := s.get("/metrics")
	s.Equal(http.StatusOK, rec.Code)
	s.Contains(rec.Body.String(), "go_goroutines",
		"the shared default registry must be exposed")
}

// The plain listener is unauthenticated, so its surface must stay /metrics
// only — every privileged ops route has to 404 here. Guards against someone
// "helpfully" mounting more handlers on the unauthenticated listener.
func (s *MetricsServerTestSuite) TestServesNothingElse() {
	for _, path := range []string{
		"/healthz", "/readyz", "/version", "/ops/acl/reload", "/debug/pprof/",
	} {
		rec := s.get(path)
		s.Equal(http.StatusNotFound, rec.Code,
			path+" must not exist on the plain metrics listener")
	}
}

func (s *MetricsServerTestSuite) TestPlainHTTP() {
	s.Nil(NewMetricsServer(":0").tlsConfig(),
		"the metrics listener must never carry TLS")
}
