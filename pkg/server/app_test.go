package server

import (
	"context"
	"crypto/x509"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/common/passhash"
	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/stretchr/testify/suite"
)

// mustHashApp hashes s with argon2id and fails the test on error.
func mustHashApp(t *testing.T, s string) string {
	t.Helper()
	h, err := passhash.HashFast(s)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

type ServerAppTestSuite struct {
	suite.Suite
}

// TestStart_ContextCancellationShutsDownGracefully verifies that cancelling
// the context passed to Start triggers a graceful stop and the function
// returns nil within a reasonable bound.
func (s *ServerAppTestSuite) TestStart_ContextCancellationShutsDownGracefully() {
	// Find a free port so the test isn't flaky on busy machines.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	port := uint(lis.Addr().(*net.TCPAddr).Port)
	lis.Close()

	cfg := &config.Config{
		Server: &config.ServerConfig{
			Address:         "127.0.0.1",
			Port:            port,
			Metrics:         false,
			MaxMessageBytes: config.DefaultMaxMessageBytes,
			Keepalive: config.ServerKeepaliveConfig{
				Time:                config.DefaultKeepaliveTime,
				Timeout:             config.DefaultKeepaliveTimeout,
				MinTime:             config.DefaultKeepaliveMinTime,
				PermitWithoutStream: config.DefaultKeepalivePermitWithoutStream,
			},
		},
		Auth: &config.BasicAuthConfig{
			AuthConfigBase: config.AuthConfigBase{Type: config.AuthConfigTypeBasic},
			Users: []config.BasicAuthConfigUser{
				{Username: "admin", PasswordHash: mustHashApp(s.T(), "admin")},
			},
		},
		Volumes: []*config.VolumeConfig{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	startErr := make(chan error, 1)
	go func() {
		startErr <- Start(ctx, cfg)
	}()

	// Give the server a moment to bind.
	s.Require().Eventually(func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 10*time.Millisecond)
		if err == nil {
			c.Close()
			return true
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "server did not bind in time")
	cancel()

	select {
	case err := <-startErr:
		s.Require().NoError(err, "graceful shutdown should not return an error")
	case <-time.After(5 * time.Second):
		s.T().Fatal("Start did not return within 5s of context cancel")
	}
}

// TestRejectIfRevoked verifies the TLS handshake hook helper:
//   - blocked leaf serial → error
//   - unblocked leaf serial → nil
//   - nil verifiedChains → nil (no client cert present)
func (s *ServerAppTestSuite) TestRejectIfRevoked() {
	rs := service.NewRevocationStore()
	rs.Set([]string{"dead"})
	leaf := &x509.Certificate{SerialNumber: big.NewInt(0xdead)}
	ok := &x509.Certificate{SerialNumber: big.NewInt(0x1234)}
	s.Require().Error(rejectIfRevoked(rs, [][]*x509.Certificate{{leaf}}))
	s.Require().NoError(rejectIfRevoked(rs, [][]*x509.Certificate{{ok}}))
	s.Require().NoError(rejectIfRevoked(rs, nil))
}

// TestNewOpsServer_WiresTLS guards the operator-mTLS wiring: ApplyTLS must run
// as part of building the ops server, so a dropped call cannot silently serve
// the mutating /ops/acl/reload endpoint as plaintext. A no-TLS config builds
// fine; a TLS config pointing at missing key material fails the build (and thus
// startup) — which only happens if ApplyTLS is actually invoked.
func (s *ServerAppTestSuite) TestNewOpsServer_WiresTLS() {
	base := func(tls config.OpsTLSConfig) *config.Config {
		return &config.Config{Server: &config.ServerConfig{Ops: config.OpsConfig{
			Addr: "127.0.0.1:0",
			TLS:  tls,
		}}}
	}

	_, err := newOpsServer(base(config.OpsTLSConfig{}), &AppContext{})
	s.Require().NoError(err) // plain HTTP ops server builds fine

	_, err = newOpsServer(base(config.OpsTLSConfig{
		CertFile: "/nonexistent/ops.crt",
		KeyFile:  "/nonexistent/ops.key",
	}), &AppContext{})
	s.Require().Error(err) // ApplyTLS is wired: a missing keypair fails the build
}

func TestServerAppTestSuite(t *testing.T) {
	suite.Run(t, new(ServerAppTestSuite))
}
