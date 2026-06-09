package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type FingerprintSuite struct{ suite.Suite }

func TestFingerprintSuite(t *testing.T) { suite.Run(t, new(FingerprintSuite)) }

// selfSignedPEM builds a minimal self-signed cert for fingerprinting.
func (s *FingerprintSuite) selfSignedPEM(cn string) []byte {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	s.Require().NoError(err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	s.Require().NoError(err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func (s *FingerprintSuite) TestStableAndSshShaped() {
	certPEM := s.selfSignedPEM("foo")
	fp1, err := Fingerprint(certPEM)
	s.Require().NoError(err)
	fp2, err := Fingerprint(certPEM)
	s.Require().NoError(err)
	s.Equal(fp1, fp2)
	s.True(strings.HasPrefix(fp1, "SHA256:"))
	s.Len(fp1[len("SHA256:"):], 43, "expected 43-char base64 raw (no padding)")
}

func (s *FingerprintSuite) TestDistinctCertsDistinctFingerprints() {
	fp1, err := Fingerprint(s.selfSignedPEM("a"))
	s.Require().NoError(err)
	fp2, err := Fingerprint(s.selfSignedPEM("b"))
	s.Require().NoError(err)
	s.NotEqual(fp1, fp2)
}

func (s *FingerprintSuite) TestNonPEMInputErrors() {
	_, err := Fingerprint([]byte("not a pem block"))
	s.Require().Error(err)
}
