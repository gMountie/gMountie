package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReloadSuite struct{ suite.Suite }

func TestReloadSuite(t *testing.T) { suite.Run(t, new(ReloadSuite)) }

const reloadYAML = `
server:
  ops:
    addr: 127.0.0.1:9090
    tls:
      cert_file: /etc/ops.crt
      key_file: /etc/ops.key
      client_ca_file: /etc/ops-ca.pem
    auth:
      type: mtls
auth:
  type: mtls
  revoked_serials: ["abcd"]
  users:
    - username: alice
      volumes: [photos]
volumes:
  - name: photos
    path: /tmp
`

func (s *ReloadSuite) TestOpsTLSParsed() {
	cfg, err := LoadConfigFromString(reloadYAML)
	s.Require().NoError(err)
	s.Equal("/etc/ops.crt", cfg.Server.Ops.TLS.CertFile)
	s.Equal("/etc/ops.key", cfg.Server.Ops.TLS.KeyFile)
	s.Equal("/etc/ops-ca.pem", cfg.Server.Ops.TLS.ClientCAFile)
	s.Equal("mtls", cfg.Server.Ops.Auth.Type)
}

func (s *ReloadSuite) TestReloadFromFile() {
	dir := s.T().TempDir()
	path := filepath.Join(dir, "server.yaml")
	s.Require().NoError(os.WriteFile(path, []byte(reloadYAML), 0o600))

	cfg, err := ReloadFromFile(path)
	s.Require().NoError(err)
	s.Equal(path, cfg.ConfigPath)
	bac, ok := cfg.Auth.(*BasicAuthConfig)
	s.Require().True(ok)
	s.Equal([]string{"abcd"}, bac.RevokedSerials)
}

func (s *ReloadSuite) TestReloadFromFile_BadPath() {
	_, err := ReloadFromFile(filepath.Join(s.T().TempDir(), "nope.yaml"))
	s.Require().Error(err)
}
