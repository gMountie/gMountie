package tls

import (
	"crypto/tls"
	"testing"

	servertls "go.gmountie.dev/gmountie/pkg/server/tls"

	"github.com/stretchr/testify/suite"
)

type SourceTestSuite struct{ suite.Suite }

func TestSourceTestSuite(t *testing.T) { suite.Run(t, new(SourceTestSuite)) }

func (s *SourceTestSuite) makeCert(host string) *tls.Certificate {
	certPEM, keyPEM, err := servertls.Generate(host)
	s.Require().NoError(err)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	s.Require().NoError(err)
	return &cert
}

func (s *SourceTestSuite) TestEmptySourceDeclinesWithoutError() {
	var m ManagedSource
	got, err := m.GetClientCertificate(nil)
	s.Require().NoError(err)
	s.Require().NotNil(got, "must return a non-nil empty cert, never nil (crypto/tls contract)")
	s.Empty(got.Certificate)
}

func (s *SourceTestSuite) TestSetThenGetReturnsCurrent() {
	var m ManagedSource
	a := s.makeCert("a")
	m.Set(a)
	got, err := m.GetClientCertificate(nil)
	s.Require().NoError(err)
	s.Equal(a, got)

	b := s.makeCert("b")
	m.Set(b)
	got, err = m.GetClientCertificate(nil)
	s.Require().NoError(err)
	s.Equal(b, got, "swap is visible to subsequent handshakes")
}
