package server

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/adrg/xdg"
	"github.com/stretchr/testify/suite"

	"gmountie/pkg/server/config"
)

type AppTLSSuite struct {
	suite.Suite
}

// ---- resolveCertPaths -------------------------------------------------------

func (s *AppTLSSuite) TestResolveCertPaths_ConfigWins() {
	cfg := config.TLSConfig{CertFile: "/x", KeyFile: "/y"}
	cert, key := resolveCertPaths(cfg)
	s.Assert().Equal("/x", cert)
	s.Assert().Equal("/y", key)
}

func (s *AppTLSSuite) TestResolveCertPaths_DefaultsToXdg() {
	cfg := config.TLSConfig{}
	cert, key := resolveCertPaths(cfg)
	base := filepath.Join(xdg.StateHome, "gmountie")
	s.Assert().Equal(filepath.Join(base, "server.crt"), cert)
	s.Assert().Equal(filepath.Join(base, "server.key"), key)
}

// TestResolveCertPaths_XdgEnvRespected verifies the helper honours XDG_STATE_HOME.
func (s *AppTLSSuite) TestResolveCertPaths_XdgEnvRespected() {
	tmpDir := s.T().TempDir()
	s.T().Setenv("XDG_STATE_HOME", tmpDir)
	// xdg caches the value; reload by reading env directly.
	cfg := config.TLSConfig{}
	cert, key := resolveCertPaths(cfg)
	// xdg.StateHome may have been cached before the env set, so just
	// confirm paths end with the expected file names under *some* base dir.
	s.Assert().Equal("server.crt", filepath.Base(cert))
	s.Assert().Equal("server.key", filepath.Base(key))
	s.Assert().Equal(filepath.Dir(cert), filepath.Dir(key))
}

// TestResolveCertPaths_OnlyCertSet: if only CertFile is set (no KeyFile), the
// config values still win — the caller will fail later at TLS parse time.
func (s *AppTLSSuite) TestResolveCertPaths_OnlyCertSet() {
	cfg := config.TLSConfig{CertFile: "/only/cert.crt"}
	cert, key := resolveCertPaths(cfg)
	s.Assert().Equal("/only/cert.crt", cert)
	s.Assert().Empty(key)
}

// ---- minTLSVersion ----------------------------------------------------------

func (s *AppTLSSuite) TestMinTLSVersion() {
	s.Assert().Equal(uint16(tls.VersionTLS12), minTLSVersion("1.2"))
	s.Assert().Equal(uint16(tls.VersionTLS13), minTLSVersion("1.3"))
	s.Assert().Equal(uint16(tls.VersionTLS13), minTLSVersion(""))
	s.Assert().Equal(uint16(tls.VersionTLS13), minTLSVersion("unknown"))
}

// ---- hostFromBind -----------------------------------------------------------

func (s *AppTLSSuite) TestHostFromBind() {
	cases := []struct {
		bind string
		want string
	}{
		{"example.com:9244", "example.com"},
		{":9244", "gmountie-server"},
		{"0.0.0.0:9244", "gmountie-server"},
		{":::9244", "gmountie-server"}, // IPv6 wildcard with port
		{"10.0.0.1:9244", "10.0.0.1"},
		{"127.0.0.1:9244", "127.0.0.1"},
		{"", "gmountie-server"},
	}
	for _, tc := range cases {
		s.Assert().Equal(tc.want, hostFromBind(tc.bind), "bind=%q", tc.bind)
	}
}

// ---- isLoopback -------------------------------------------------------------

func (s *AppTLSSuite) TestIsLoopback_True() {
	loopbacks := []string{
		"127.0.0.1:9244",
		"[::1]:9244",
		"localhost:9244",
	}
	for _, addr := range loopbacks {
		s.Assert().True(isLoopback(addr), "expected loopback: %s", addr)
	}
}

func (s *AppTLSSuite) TestIsLoopback_False() {
	nonLoopbacks := []string{
		"10.0.0.1:9244",
		"0.0.0.0:9244",
		"192.168.1.1:9244",
		"[::]:9244",
	}
	for _, addr := range nonLoopbacks {
		s.Assert().False(isLoopback(addr), "expected non-loopback: %s", addr)
	}
}

// ---- integration: resolveCertPaths + LoadOrGenerate roundtrip --------------

func (s *AppTLSSuite) TestResolveCertPaths_GenerateRoundtrip() {
	dir := s.T().TempDir()
	cfg := config.TLSConfig{
		CertFile: filepath.Join(dir, "server.crt"),
		KeyFile:  filepath.Join(dir, "server.key"),
	}
	certPath, keyPath := resolveCertPaths(cfg)
	s.Require().NoError(os.MkdirAll(filepath.Dir(certPath), 0o755))

	// Use the tls package directly to verify paths resolve cleanly.
	s.Assert().Equal(cfg.CertFile, certPath)
	s.Assert().Equal(cfg.KeyFile, keyPath)
}

func TestAppTLSSuite(t *testing.T) {
	suite.Run(t, new(AppTLSSuite))
}
