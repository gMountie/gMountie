package config

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/common/passhash"

	"github.com/stretchr/testify/suite"
)

func TestServerOpsConfigSuite(t *testing.T) {
	suite.Run(t, new(ServerOpsConfigSuite))
}

type ServerOpsConfigSuite struct {
	suite.Suite
}

func mustHashOps(t *testing.T, s string) string {
	t.Helper()
	h, err := passhash.HashFast(s)
	if err != nil {
		t.Fatalf("passhash.Hash: %v", err)
	}
	return h
}

func (s *ServerOpsConfigSuite) TestDefaultOpsAddr() {
	cfg, err := LoadConfigFromString(minimalConfig())
	s.Require().NoError(err)
	s.Equal("127.0.0.1:9090", cfg.Server.Ops.Addr)
}

func (s *ServerOpsConfigSuite) TestDefaultOpsAuthType() {
	cfg, err := LoadConfigFromString(minimalConfig())
	s.Require().NoError(err)
	s.Equal("none", cfg.Server.Ops.Auth.Type)
}

func (s *ServerOpsConfigSuite) TestMetricsAddrDefaultEmpty() {
	cfg, err := LoadConfigFromString(minimalConfig())
	s.Require().NoError(err)
	s.Empty(cfg.Server.PlainMetricsAddr,
		"the plain metrics listener must be disabled by default")
}

func (s *ServerOpsConfigSuite) TestMetricsAddrParsed() {
	yaml := minimalConfig() + `
server:
  plain_metrics_addr: ":9091"
`
	cfg, err := LoadConfigFromString(yaml)
	s.Require().NoError(err)
	s.Equal(":9091", cfg.Server.PlainMetricsAddr)
}

func (s *ServerOpsConfigSuite) TestExplicitOpsAddrOverride() {
	yaml := minimalConfig() + `
server:
  ops:
    addr: "127.0.0.1:19090"
`
	cfg, err := LoadConfigFromString(yaml)
	s.Require().NoError(err)
	s.Equal("127.0.0.1:19090", cfg.Server.Ops.Addr)
}

func (s *ServerOpsConfigSuite) TestBasicAuthUsersUnmarshal() {
	hash := mustHashOps(s.T(), "testpass")
	yaml := minimalConfig() + `
server:
  ops:
    addr: "127.0.0.1:9090"
    auth:
      type: basic
      users:
        - username: opsuser
          password_hash: "` + hash + `"
`
	cfg, err := LoadConfigFromString(yaml)
	s.Require().NoError(err)
	s.Equal("basic", cfg.Server.Ops.Auth.Type)
	s.Require().Len(cfg.Server.Ops.Auth.Users, 1)
	s.Equal("opsuser", cfg.Server.Ops.Auth.Users[0].Username)
	s.Equal(hash, cfg.Server.Ops.Auth.Users[0].PasswordHash)
}

// minimalConfig returns the smallest valid server config YAML (auth + one
// volume are required by the validator).
func minimalConfig() string {
	return `
auth:
  type: basic
  users:
    - username: admin
      password_hash: "$argon2id$v=19$m=65536,t=3,p=4$AAAAAAAAAAAAAAAA$47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU"
volumes:
  - name: test
    path: /tmp
`
}
