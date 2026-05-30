package service_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"gmountie/pkg/server/config"
	"gmountie/pkg/server/service"
)

// MTLSAuthSuite tests the mtlsAuthService principal-extraction logic.
// Cert construction uses bare x509.Certificate literals — no crypto/ecdsa
// ceremony needed because Authorize only reads VerifiedChains, which is
// populated by the caller (ctxWithClientCert), not by actual TLS handshake.
type MTLSAuthSuite struct {
	suite.Suite
	svc service.AuthService
}

func TestMTLSAuthSuite(t *testing.T) {
	suite.Run(t, new(MTLSAuthSuite))
}

func (s *MTLSAuthSuite) SetupTest() {
	s.svc = service.NewAuthServiceFromConfig(&config.BasicAuthConfig{
		AuthConfigBase: config.AuthConfigBase{Type: config.AuthConfigTypeMTLS},
	})
}

// ctxWithClientCert builds a context carrying a peer whose TLSInfo contains
// the given certificate as the sole leaf in VerifiedChains.
func ctxWithClientCert(cert *x509.Certificate) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{
				VerifiedChains: [][]*x509.Certificate{{cert}},
			},
		},
	})
}

// ctxWithEmptyChains carries TLSInfo but no verified chain (simulates a
// connection where the TLS layer ran but no client cert was presented).
func ctxWithEmptyChains() context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{
				VerifiedChains: [][]*x509.Certificate{},
			},
		},
	})
}

// TestCN_Alice — leaf has CN "alice"; principal should be "alice".
func (s *MTLSAuthSuite) TestCN_Alice() {
	cert := &x509.Certificate{
		Subject: pkix.Name{CommonName: "alice"},
	}
	details, err := s.svc.Authorize(ctxWithClientCert(cert), "/any.Service/Method")
	s.Require().NoError(err)
	s.Require().NotNil(details)
	s.Equal("alice", details.Username)
}

// TestDNSSAN_Bob — CN is empty; first DNS SAN "bob" becomes the principal.
func (s *MTLSAuthSuite) TestDNSSAN_Bob() {
	cert := &x509.Certificate{
		DNSNames: []string{"bob", "bob.example.com"},
	}
	details, err := s.svc.Authorize(ctxWithClientCert(cert), "/any.Service/Method")
	s.Require().NoError(err)
	s.Require().NotNil(details)
	s.Equal("bob", details.Username)
}

// TestEmptyVerifiedChains — TLSInfo present but no verified chain →
// Unauthenticated (no verified client certificate).
func (s *MTLSAuthSuite) TestEmptyVerifiedChains() {
	details, err := s.svc.Authorize(ctxWithEmptyChains(), "/any.Service/Method")
	s.Nil(details)
	s.Require().Error(err)
	s.Equal(codes.Unauthenticated, status.Code(err))
}

// TestNonTLSAuthInfo — peer has non-TLS AuthInfo → Unauthenticated
// (connection is not mTLS).
type fakeAuthInfo struct{}

func (fakeAuthInfo) AuthType() string { return "fake" }

func (s *MTLSAuthSuite) TestNonTLSAuthInfo() {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: fakeAuthInfo{},
	})
	details, err := s.svc.Authorize(ctx, "/any.Service/Method")
	s.Nil(details)
	s.Require().Error(err)
	s.Equal(codes.Unauthenticated, status.Code(err))
}

// TestNoPeerInfo — no peer in context → Unauthenticated (no peer info).
func (s *MTLSAuthSuite) TestNoPeerInfo() {
	details, err := s.svc.Authorize(context.Background(), "/any.Service/Method")
	s.Nil(details)
	s.Require().Error(err)
	s.Equal(codes.Unauthenticated, status.Code(err))
}
