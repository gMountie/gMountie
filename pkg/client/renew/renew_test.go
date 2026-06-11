package renew

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	clienttls "go.gmountie.dev/gmountie/pkg/client/tls"

	"github.com/stretchr/testify/suite"
)

type RenewTestSuite struct {
	suite.Suite
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caPEM  string
	srv    *httptest.Server
	// captured/served by the handler
	lastAuth string
	subject  string
	sans     []string
	ttl      time.Duration
	// per-test overrides; nil means use the default handler behaviour
	overrideProfileCAOverride *string                                                   // when non-nil, replaces ca_pem in /profile response
	overrideSignFn            func(w http.ResponseWriter, csr *x509.CertificateRequest) // when non-nil, replaces the default cert-signing logic
}

func TestRenewTestSuite(t *testing.T) { suite.Run(t, new(RenewTestSuite)) }

func (s *RenewTestSuite) SetupSuite() {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	s.Require().NoError(err)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	s.Require().NoError(err)
	s.caCert, err = x509.ParseCertificate(der)
	s.Require().NoError(err)
	s.caKey = key
	s.caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func (s *RenewTestSuite) SetupTest() {
	s.subject, s.sans, s.ttl = "alice", []string{"alice", "gmountie://pat/pat_x1", "gmountie://vol/*"}, time.Hour
	s.overrideProfileCAOverride = nil
	s.overrideSignFn = nil
	mux := http.NewServeMux()
	mux.HandleFunc("GET /profile", func(w http.ResponseWriter, r *http.Request) {
		s.lastAuth = r.Header.Get("Authorization")
		caPEM := s.caPEM
		if s.overrideProfileCAOverride != nil {
			caPEM = *s.overrideProfileCAOverride
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"subject": s.subject, "sans": s.sans, "ca_pem": caPEM,
		})
	})
	mux.HandleFunc("POST /renew", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CSRPEM string `json:"csr_pem"`
		}
		s.Require().NoError(json.NewDecoder(r.Body).Decode(&req))
		block, _ := pem.Decode([]byte(req.CSRPEM))
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		s.Require().NoError(err)
		s.Require().NoError(csr.CheckSignature())
		if s.overrideSignFn != nil {
			s.overrideSignFn(w, csr)
			return
		}
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      csr.Subject, DNSNames: csr.DNSNames, URIs: csr.URIs,
			NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(s.ttl),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, s.caCert, csr.PublicKey, s.caKey)
		s.Require().NoError(err)
		chain := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cert_chain_pem": chain, "ca_pem": s.caPEM, "not_after": leaf.NotAfter,
		})
	})
	s.srv = httptest.NewTLSServer(mux)
}

func (s *RenewTestSuite) TearDownTest() { s.srv.Close() }

func (s *RenewTestSuite) newRefresher(src *clienttls.ManagedSource) *Refresher {
	r := New(Config{Endpoint: s.srv.URL, Token: "tok", Before: 30 * time.Minute}, src)
	r.client = s.srv.Client() // trust the httptest TLS cert
	return r
}

func (s *RenewTestSuite) TestRenewNowMintsAndSwaps() {
	var src clienttls.ManagedSource
	r := s.newRefresher(&src)
	s.Require().NoError(r.RenewNow(s.T().Context()))

	s.Equal("Bearer tok", s.lastAuth)
	cur := src.Current()
	s.Require().NotNil(cur)
	s.Require().NotNil(cur.Leaf)
	s.Equal("alice", cur.Leaf.Subject.CommonName)
	s.Equal([]string{"alice"}, cur.Leaf.DNSNames)
	var uris []string
	for _, u := range cur.Leaf.URIs {
		uris = append(uris, u.String())
	}
	s.ElementsMatch([]string{"gmountie://pat/pat_x1", "gmountie://vol/*"}, uris)
	s.Equal(s.caPEM, r.CAPEM())
	s.WithinDuration(time.Now().Add(s.ttl), r.NotAfter(), time.Minute)
	s.IsType(&ecdsa.PrivateKey{}, cur.PrivateKey, "key generated client-side, in memory")
}

func (s *RenewTestSuite) TestSANClassification() {
	uris, dns := classifySANs([]string{"alice", "gmountie://vol/abc"})
	s.Equal([]string{"alice"}, dns)
	s.Require().Len(uris, 1)
	s.Equal("gmountie://vol/abc", uris[0].String())
}

func (s *RenewTestSuite) TestRenewNowSurfacesHTTPError() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /profile", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401) })
	srv := httptest.NewTLSServer(mux)
	defer srv.Close()
	var src clienttls.ManagedSource
	r := New(Config{Endpoint: srv.URL, Token: "bad", Before: time.Minute}, &src)
	r.client = srv.Client()
	err := r.RenewNow(s.T().Context())
	s.Require().Error(err)
	s.Contains(err.Error(), "401")
	s.Nil(src.Current(), "no swap on failure")
}

func (s *RenewTestSuite) TestTokenFileWinsAndIsReread() {
	dir := s.T().TempDir()
	path := dir + "/tok"
	s.Require().NoError(os.WriteFile(path, []byte("file-tok-1\n"), 0o600))
	var src clienttls.ManagedSource
	r := New(Config{Endpoint: s.srv.URL, Token: "inline-ignored", TokenFile: path, Before: time.Minute}, &src)
	r.client = s.srv.Client()
	s.Require().NoError(r.RenewNow(s.T().Context()))
	s.Equal("Bearer file-tok-1", s.lastAuth, "token_file wins over inline and is trimmed")
	s.Require().NoError(os.WriteFile(path, []byte("file-tok-2"), 0o600))
	s.Require().NoError(r.RenewNow(s.T().Context()))
	s.Equal("Bearer file-tok-2", s.lastAuth, "token re-read on every exchange")
}

// Finding 1a: server signs a DIFFERENT keypair than the CSR's key — must error,
// must not swap into the source.
func (s *RenewTestSuite) TestKeyMismatchRejected() {
	// The server signs with a freshly generated key instead of the CSR's public
	// key, simulating a hostile or buggy CA returning a cert for a different key.
	s.overrideSignFn = func(w http.ResponseWriter, _ *x509.CertificateRequest) {
		wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		s.Require().NoError(err)
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject:      pkix.Name{CommonName: s.subject},
			NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(s.ttl),
			KeyUsage: x509.KeyUsageDigitalSignature,
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, s.caCert, &wrongKey.PublicKey, s.caKey)
		s.Require().NoError(err)
		chain := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cert_chain_pem": chain, "ca_pem": s.caPEM, "not_after": leaf.NotAfter,
		})
	}
	var src clienttls.ManagedSource
	r := s.newRefresher(&src)
	err := r.RenewNow(s.T().Context())
	s.Require().Error(err)
	s.Contains(err.Error(), "mismatch", "error message must describe the key mismatch")
	s.Nil(src.Current(), "source must not be swapped on key mismatch")
}

// Finding 1b: server returns a leaf whose CN does not match the profile subject.
func (s *RenewTestSuite) TestCNMismatchRejected() {
	s.overrideSignFn = func(w http.ResponseWriter, csr *x509.CertificateRequest) {
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(2),
			Subject:      pkix.Name{CommonName: "mallory"}, // wrong CN
			NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(s.ttl),
			KeyUsage: x509.KeyUsageDigitalSignature,
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, s.caCert, csr.PublicKey, s.caKey)
		s.Require().NoError(err)
		chain := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cert_chain_pem": chain, "ca_pem": s.caPEM, "not_after": leaf.NotAfter,
		})
	}
	var src clienttls.ManagedSource
	r := s.newRefresher(&src)
	err := r.RenewNow(s.T().Context())
	s.Require().Error(err)
	s.Contains(err.Error(), "CN", "error message must describe the CN mismatch")
	s.Nil(src.Current(), "source must not be swapped on CN mismatch")
}

// Finding 2: /renew response omits ca_pem entirely; CAPEM() must return the
// profile's CA.
func (s *RenewTestSuite) TestProfileCAPEMUsedWhenSignResponseOmitsIt() {
	s.overrideSignFn = func(w http.ResponseWriter, csr *x509.CertificateRequest) {
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(3),
			Subject:      pkix.Name{CommonName: s.subject},
			NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(s.ttl),
			KeyUsage: x509.KeyUsageDigitalSignature,
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, s.caCert, csr.PublicKey, s.caKey)
		s.Require().NoError(err)
		chain := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		// Deliberately omit ca_pem from the sign response.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cert_chain_pem": chain, "not_after": leaf.NotAfter,
		})
	}
	var src clienttls.ManagedSource
	r := s.newRefresher(&src)
	s.Require().NoError(r.RenewNow(s.T().Context()))
	s.Equal(s.caPEM, r.CAPEM(), "profile CA must be stored even when sign response omits ca_pem")
}

// Finding 3a: server appends the CA cert after the leaf in the chain PEM.
// Current().Certificate must contain both DER blocks; NotAfter tracks the leaf.
func (s *RenewTestSuite) TestMultiCertChainLeafFirst() {
	s.overrideSignFn = func(w http.ResponseWriter, csr *x509.CertificateRequest) {
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(4),
			Subject:      pkix.Name{CommonName: s.subject},
			NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(s.ttl),
			KeyUsage: x509.KeyUsageDigitalSignature,
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leaf, s.caCert, csr.PublicKey, s.caKey)
		s.Require().NoError(err)
		// Append CA cert as second block (common chain format).
		chain := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})) +
			s.caPEM
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cert_chain_pem": chain, "ca_pem": s.caPEM, "not_after": leaf.NotAfter,
		})
	}
	var src clienttls.ManagedSource
	r := s.newRefresher(&src)
	s.Require().NoError(r.RenewNow(s.T().Context()))
	cur := src.Current()
	s.Require().NotNil(cur)
	s.Len(cur.Certificate, 2, "both leaf and CA DER blocks must be present")
	s.WithinDuration(time.Now().Add(s.ttl), r.NotAfter(), time.Minute, "NotAfter tracks the leaf, not the CA")
}

func (s *RenewTestSuite) TestRunRenewsBeforeExpiry() {
	s.ttl = 500 * time.Millisecond // leaf expires fast
	var src clienttls.ManagedSource
	r := s.newRefresher(&src)
	r.cfg.Before = 400 * time.Millisecond // renew when <400ms left → ~100ms cadence
	r.retryMin, r.retryMax = 50*time.Millisecond, 200*time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Require().NoError(r.RenewNow(ctx))
	first := src.Current()

	go r.Run(ctx)
	s.Require().Eventually(func() bool {
		cur := src.Current()
		return cur != nil && cur != first
	}, 5*time.Second, 20*time.Millisecond, "loop must mint a fresh cert before expiry")
}

func (s *RenewTestSuite) TestRunStartsWithNoCertAndMintsImmediately() {
	var src clienttls.ManagedSource
	r := s.newRefresher(&src)
	r.retryMin = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx) // NotAfter is zero → first iteration renews immediately
	s.Require().Eventually(func() bool { return src.Current() != nil },
		5*time.Second, 10*time.Millisecond)
}

func (s *RenewTestSuite) TestRunBacksOffOnFailureAndStopsOnCancel() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /profile", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
	srv := httptest.NewTLSServer(mux)
	defer srv.Close()
	var src clienttls.ManagedSource
	r := New(Config{Endpoint: srv.URL, Token: "t", Before: time.Hour}, &src)
	r.client = srv.Client()
	r.retryMin, r.retryMax = 10*time.Millisecond, 40*time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	time.Sleep(100 * time.Millisecond) // a few failing rounds
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		s.Fail("Run did not stop on context cancel")
	}
	s.Nil(src.Current(), "failures never swapped anything in")
}

// Overflow/saturation guard: when notAfter is ~300 years in the past,
// time.Until saturates near MinInt64 and the naive subtraction "time.Until -
// Before" wraps positive, sending the loop into a ~292-year sleep while
// holding an expired cert. leadUntilRenew must return negative so Run calls
// RenewNow immediately.
func (s *RenewTestSuite) TestRunRenewsWhenNotAfterFarInPast() {
	var src clienttls.ManagedSource
	r := s.newRefresher(&src)
	r.retryMin, r.retryMax = 20*time.Millisecond, 50*time.Millisecond

	// Force notAfter to 300 years in the past under mu so the first iteration
	// sees a long-expired cert without going through a real exchange.
	r.mu.Lock()
	r.notAfter = time.Now().AddDate(-300, 0, 0)
	r.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	// If the saturation bug is present, lead wraps positive → sleep ~292 years.
	// If the fix is in place, lead is negative → RenewNow fires immediately.
	s.Require().Eventually(func() bool { return src.Current() != nil },
		5*time.Second, 20*time.Millisecond,
		"Run must renew immediately when notAfter is far in the past (overflow guard)")
}

// Finding 3b: server returns empty cert_chain_pem → error containing "no
// certificates", no swap.
func (s *RenewTestSuite) TestEmptyCertChainErrors() {
	s.overrideSignFn = func(w http.ResponseWriter, _ *x509.CertificateRequest) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cert_chain_pem": "", "ca_pem": s.caPEM,
		})
	}
	var src clienttls.ManagedSource
	r := s.newRefresher(&src)
	err := r.RenewNow(s.T().Context())
	s.Require().Error(err)
	s.Contains(err.Error(), "no certificates", "error must mention missing certificates")
	s.Nil(src.Current(), "source must not be swapped when chain is empty")
}
