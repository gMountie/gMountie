package tls

import (
	"crypto/tls"
	"sync"
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

func (s *SourceTestSuite) TestConcurrentSetAndGet() {
	var m ManagedSource
	a, b := s.makeCert("a"), s.makeCert("b")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if (i+j)%2 == 0 {
					m.Set(a)
				} else {
					m.Set(b)
				}
			}
		}(i)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				got, err := m.GetClientCertificate(nil)
				s.Assert().NoError(err)
				s.Assert().NotNil(got)
				if got != a && got != b && len(got.Certificate) != 0 {
					s.Failf("unexpected cert", "got %p", got)
				}
			}
		}()
	}
	wg.Wait()
}
