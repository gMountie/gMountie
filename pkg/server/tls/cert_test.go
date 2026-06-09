package tls

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type CertSuite struct{ suite.Suite }

func TestCertSuite(t *testing.T) { suite.Run(t, new(CertSuite)) }

func (s *CertSuite) TestGenerateSelfSignedECDSA() {
	certPEM, keyPEM, err := Generate("test.example.com")
	s.Require().NoError(err)
	s.Require().NotEmpty(certPEM)
	s.Require().NotEmpty(keyPEM)

	block, _ := pem.Decode(certPEM)
	s.Require().NotNil(block)
	c, err := x509.ParseCertificate(block.Bytes)
	s.Require().NoError(err)
	s.Equal("test.example.com", c.Subject.CommonName)
	s.Contains(c.DNSNames, "test.example.com")
	s.True(c.NotAfter.After(time.Now().Add(9*365*24*time.Hour)),
		"10-year validity expected, got NotAfter %v", c.NotAfter)
	_, ok := c.PublicKey.(*ecdsa.PublicKey)
	s.True(ok, "expected ECDSA public key")
}

func (s *CertSuite) TestLoadFromDisk() {
	dir := s.T().TempDir()
	certPath := filepath.Join(dir, "c.crt")
	keyPath := filepath.Join(dir, "c.key")
	certPEM, keyPEM, err := Generate("loaded")
	s.Require().NoError(err)
	s.Require().NoError(os.WriteFile(certPath, certPEM, 0o644))
	s.Require().NoError(os.WriteFile(keyPath, keyPEM, 0o600))

	cert, gotPEM, err := Load(certPath, keyPath)
	s.Require().NoError(err)
	s.Equal(certPEM, gotPEM)
	s.NotNil(cert.PrivateKey)
	// cert is already a stdtls.Certificate (Load's return type) — no conversion needed.
}

func (s *CertSuite) TestLoadOrGenerate_FirstStartCreatesFiles() {
	dir := s.T().TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	_, _, fp1, err := LoadOrGenerate(certPath, keyPath, "host-1")
	s.Require().NoError(err)
	s.NotEmpty(fp1)

	st, err := os.Stat(keyPath)
	s.Require().NoError(err)
	s.Equal(os.FileMode(0o600), st.Mode().Perm(), "key file mode must be 0600")
	st, err = os.Stat(certPath)
	s.Require().NoError(err)
	s.Equal(os.FileMode(0o644), st.Mode().Perm())
}

func (s *CertSuite) TestLoadOrGenerate_SubsequentLoadDoesNotRegenerate() {
	dir := s.T().TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	_, _, fp1, err := LoadOrGenerate(certPath, keyPath, "host")
	s.Require().NoError(err)
	_, _, fp2, err := LoadOrGenerate(certPath, keyPath, "host")
	s.Require().NoError(err)
	s.Equal(fp1, fp2, "second call must NOT regenerate the cert")
}
