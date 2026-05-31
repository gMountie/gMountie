package ops

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	serverconfig "go.gmountie.dev/gmountie/pkg/server/config"
	servertls "go.gmountie.dev/gmountie/pkg/server/tls"
)

type ServerTestSuite struct{ suite.Suite }

// muxOf reaches into a freshly-built Server, pulls its Handler, and
// runs requests against it via httptest. Avoids actually opening a
// listener while still exercising real routing.
func muxOf(s *Server) http.Handler { return s.server.Handler }

// writeOpsCertKeyCA generates two independent self-signed ECDSA certs via
// the existing servertls.Generate helper and writes them to temp files.
// Returns (certFile, keyFile, caFile) for use in ApplyTLS / NewServerWithTLS.
// The "CA" file is simply a second self-signed cert — AppendCertsFromPEM only
// requires a parseable PEM block, not an actual CA flag, for the structural
// assertion we're making here.
func writeOpsCertKeyCA(t *testing.T, dir string) (certFile, keyFile, caFile string) {
	t.Helper()

	certPEM, keyPEM, err := servertls.Generate("127.0.0.1")
	if err != nil {
		t.Fatalf("generate leaf cert: %v", err)
	}
	caPEM, _, err := servertls.Generate("ops-test-ca")
	if err != nil {
		t.Fatalf("generate ca cert: %v", err)
	}

	certFile = filepath.Join(dir, "ops.crt")
	keyFile = filepath.Join(dir, "ops.key")
	caFile = filepath.Join(dir, "ops-ca.crt")

	if err := os.WriteFile(certFile, certPEM, 0o644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.WriteFile(caFile, caPEM, 0o644); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	return certFile, keyFile, caFile
}

func (s *ServerTestSuite) TestPprofDisabledByDefault() {
	srv := NewServer(":0", stubReadiness{}, false, nil, nil, nil, nil, nil)
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		muxOf(srv).ServeHTTP(rr, req)
		s.Assert().Equal(http.StatusNotFound, rr.Code,
			"pprof leaked at %s with enablePprof=false", path)
	}
}

func (s *ServerTestSuite) TestPprofEnabledServesIndex() {
	srv := NewServer(":0", stubReadiness{}, true, nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rr := httptest.NewRecorder()
	muxOf(srv).ServeHTTP(rr, req)
	s.Assert().Equal(http.StatusOK, rr.Code)
	// pprof.Index renders an HTML page listing the profile names —
	// "goroutine" is the most stable substring across Go versions.
	s.Assert().Contains(rr.Body.String(), "goroutine")
}

func (s *ServerTestSuite) TestPprofEnabledLeavesCoreRoutesIntact() {
	srv := NewServer(":0", stubReadiness{}, true, nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	muxOf(srv).ServeHTTP(rr, req)
	s.Assert().Equal(http.StatusOK, rr.Code)
}

func (s *ServerTestSuite) TestOpsMTLSConfigured() {
	dir := s.T().TempDir()
	certFile, keyFile, caFile := writeOpsCertKeyCA(s.T(), dir)
	srv, err := NewServerWithTLS("127.0.0.1:0", serverconfig.OpsTLSConfig{
		CertFile:     certFile,
		KeyFile:      keyFile,
		ClientCAFile: caFile,
	})
	s.Require().NoError(err)
	tc := srv.tlsConfig()
	s.Require().NotNil(tc)
	s.Equal(tls.RequireAndVerifyClientCert, tc.ClientAuth)
	s.Require().NotNil(tc.ClientCAs)
}

func (s *ServerTestSuite) TestOpsTLSEmptyIsPlainHTTP() {
	srv, err := NewServerWithTLS("127.0.0.1:0", serverconfig.OpsTLSConfig{})
	s.Require().NoError(err)
	s.Nil(srv.tlsConfig()) // no cert/key ⇒ plain HTTP
}

func TestServerTestSuite(t *testing.T) { suite.Run(t, new(ServerTestSuite)) }
