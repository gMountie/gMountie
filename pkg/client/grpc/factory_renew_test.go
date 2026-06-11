package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/renew"
	clienttls "go.gmountie.dev/gmountie/pkg/client/tls"
	commonconfig "go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/controller"
	"go.gmountie.dev/gmountie/pkg/server/service"
	servertls "go.gmountie.dev/gmountie/pkg/server/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// renewFixture bundles a self-signed test CA, a token→cert exchange server, and
// an mTLS gRPC server that requires a client cert signed by that CA. It is the
// minimal end-to-end environment for "a client configured ONLY with renew.*
// connects to a server requiring client certs".
type renewFixture struct {
	caCert   *x509.Certificate
	caKey    *ecdsa.PrivateKey
	caPEM    string
	exchange *httptest.Server
	grpcSrv  *grpc.Server
	grpcAddr string
}

// newRenewFixture builds the CA, exchange server, and mTLS gRPC server. If
// exchangeStatus is non-zero, every exchange endpoint returns that status code
// (used to drive the initial-exchange failure assertion).
func (s *FactoryTestSuite) newRenewFixture(exchangeStatus int) *renewFixture {
	return s.newRenewFixtureCA(exchangeStatus, true)
}

// newRenewFixtureCA is newRenewFixture with control over whether the exchange
// returns the CA PEM. When withCA is false both /profile and /renew return an
// empty "ca_pem", exercising the no-CA-delivered token-mode path.
func (s *FactoryTestSuite) newRenewFixtureCA(exchangeStatus int, withCA bool) *renewFixture {
	f := &renewFixture{}

	// Self-signed CA (mirrors pkg/client/renew/renew_test.go's SetupSuite).
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	s.Require().NoError(err)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	s.Require().NoError(err)
	f.caCert, err = x509.ParseCertificate(der)
	s.Require().NoError(err)
	f.caKey = key
	f.caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))

	// caInExchange is the ca_pem the exchange advertises: the real CA, or empty
	// when withCA is false (to drive the no-CA-delivered token-mode path).
	caInExchange := f.caPEM
	if !withCA {
		caInExchange = ""
	}

	// Token→certificate exchange server (mirrors the renew_test.go handler).
	mux := http.NewServeMux()
	mux.HandleFunc("GET /profile", func(w http.ResponseWriter, _ *http.Request) {
		if exchangeStatus != 0 {
			w.WriteHeader(exchangeStatus)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"subject": "client-1", "sans": []string{"client-1"}, "ca_pem": caInExchange,
		})
	})
	mux.HandleFunc("POST /renew", func(w http.ResponseWriter, r *http.Request) {
		if exchangeStatus != 0 {
			w.WriteHeader(exchangeStatus)
			return
		}
		var req struct {
			CSRPEM string `json:"csr_pem"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		block, _ := pem.Decode([]byte(req.CSRPEM))
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := csr.CheckSignature(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      csr.Subject, DNSNames: csr.DNSNames, URIs: csr.URIs,
			NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leaf, f.caCert, csr.PublicKey, f.caKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		chain := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cert_chain_pem": chain, "ca_pem": caInExchange, "not_after": leaf.NotAfter,
		})
	})
	f.exchange = httptest.NewTLSServer(mux)

	// mTLS gRPC server that requires a client cert signed by the test CA.
	certPEM, keyPEM, err := servertls.Generate("127.0.0.1")
	s.Require().NoError(err)
	serverCert, err := tls.X509KeyPair(certPEM, keyPEM)
	s.Require().NoError(err)
	pool := x509.NewCertPool()
	s.Require().True(pool.AppendCertsFromPEM([]byte(f.caPEM)))
	creds := credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h2"},
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	srv := grpc.NewServer(grpc.Creds(creds))
	sessMgr := service.NewSessionManager(service.SessionManagerOptions{})
	proto.RegisterSessionServiceServer(srv, controller.NewSessionController(sessMgr, nil))
	go func() { _ = srv.Serve(lis) }()
	f.grpcSrv = srv
	f.grpcAddr = lis.Addr().String()
	return f
}

func (f *renewFixture) close() {
	f.exchange.Close()
	f.grpcSrv.Stop()
}

// renewConfig builds a config configured ONLY with renew.* — no static client
// cert anywhere. Server-identity verification is off (insecure) since it is not
// under test here; the assertion is that the in-memory client cert is presented
// and accepted by the mTLS server.
func (s *FactoryTestSuite) renewConfig(f *renewFixture) *config.Config {
	host, portStr, err := net.SplitHostPort(f.grpcAddr)
	s.Require().NoError(err)
	port, err := strconv.Atoi(portStr)
	s.Require().NoError(err)
	return &config.Config{
		Server: &config.ServerConfig{
			Address: host,
			Port:    uint(port),
			TLS:     config.TLSConfig{Verify: "insecure"},
		},
		Auth:  &commonconfig.AuthConfigBase{Type: commonconfig.AuthConfigTypeMTLS},
		Renew: &config.RenewConfig{Endpoint: f.exchange.URL, Token: "tok", Before: time.Hour},
	}
}

// withRefresherTrust swaps newRefresherFn so the constructed refresher trusts
// the httptest exchange server's self-signed cert, then returns a restore func.
// From package grpc we cannot reach the refresher's unexported HTTP client, so
// this uses the exported SetHTTPClient seam.
func (s *FactoryTestSuite) withRefresherTrust(f *renewFixture) func() {
	prev := newRefresherFn
	newRefresherFn = func(cfg renew.Config, src *clienttls.ManagedSource) *renew.Refresher {
		r := renew.New(cfg, src)
		r.SetHTTPClient(f.exchange.Client())
		return r
	}
	return func() { newRefresherFn = prev }
}

// TestTokenMode_ConnectsToMTLSServer is the end-to-end token-mode test: a
// client with no static cert, only renew.*, does the synchronous initial
// exchange, presents the minted in-memory cert, and completes a real session
// handshake over the mTLS link.
func (s *FactoryTestSuite) TestTokenMode_ConnectsToMTLSServer() {
	f := s.newRenewFixture(0)
	defer f.close()
	restore := s.withRefresherTrust(f)
	defer restore()

	cfg := s.renewConfig(f)
	c, err := newUnconnectedClient(cfg, f.grpcAddr)
	s.Require().NoError(err)
	defer c.Close()

	// Force the connection + a real RPC over the mTLS link. Connect runs the
	// SessionService/Create handshake, which only succeeds if the server
	// accepted the in-memory client certificate.
	s.Require().NoError(c.Connect())
	s.NotEmpty(c.SessionID(), "session handshake must succeed over the mTLS link")
}

// TestTokenMode_MutualExclusion verifies a renew block combined with a static
// client cert is rejected before any exchange.
func (s *FactoryTestSuite) TestTokenMode_MutualExclusion() {
	f := s.newRenewFixture(0)
	defer f.close()
	restore := s.withRefresherTrust(f)
	defer restore()

	cfg := s.renewConfig(f)
	cfg.Server.TLS.CertPEM = "some-static-cert"
	c, err := newUnconnectedClient(cfg, f.grpcAddr)
	s.Require().Error(err)
	s.Nil(c)
	s.Contains(err.Error(), "mutually exclusive")
}

// TestTokenMode_InitialExchangeFailureSurfaces verifies a failed initial
// exchange (exchange server returning 500) is surfaced by newUnconnectedClient
// with the "initial certificate exchange" wrap and no client is returned.
func (s *FactoryTestSuite) TestTokenMode_InitialExchangeFailureSurfaces() {
	f := s.newRenewFixture(http.StatusInternalServerError)
	defer f.close()
	restore := s.withRefresherTrust(f)
	defer restore()

	cfg := s.renewConfig(f)
	c, err := newUnconnectedClient(cfg, f.grpcAddr)
	s.Require().Error(err)
	s.Nil(c)
	s.Contains(err.Error(), "initial certificate exchange")
}

// TestTokenMode_NoCADeliveredErrorsInVerifyMode verifies that when the exchange
// returns an empty ca_pem and no CA is configured (and the effective verify mode
// does full chain verification with no fingerprint pin), newUnconnectedClient
// returns a loud error instead of silently falling back to WebPKI roots.
func (s *FactoryTestSuite) TestTokenMode_NoCADeliveredErrorsInVerifyMode() {
	f := s.newRenewFixtureCA(0, false)
	defer f.close()
	restore := s.withRefresherTrust(f)
	defer restore()

	cfg := s.renewConfig(f)
	cfg.Server.TLS.Verify = "verify" // full chain verify, no CA, no pin
	c, err := newUnconnectedClient(cfg, f.grpcAddr)
	s.Require().Error(err)
	s.Nil(c)
	s.Contains(err.Error(), "no ca_pem")
}

// TestWithBackgroundTask_StopsOnClose verifies a registered background task is
// launched on construction and returns when Close cancels the lifecycle ctx.
func (s *FactoryTestSuite) TestWithBackgroundTask_StopsOnClose() {
	started := make(chan struct{})
	stopped := make(chan struct{})
	task := func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(stopped)
	}
	client, err := NewClient(s.endpoint(),
		WithDialOptions([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}),
		WithBackgroundTask(task),
	)
	s.Require().NoError(err)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		s.Fail("background task did not start on construction")
	}

	s.Require().NoError(client.Close())
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		s.Fail("background task did not stop on Close")
	}
}
