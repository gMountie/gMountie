package server

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/server/config"
)

func TestAppOpsSuite(t *testing.T) {
	suite.Run(t, new(AppOpsSuite))
}

type AppOpsSuite struct {
	suite.Suite
}

func (s *AppOpsSuite) TestValidateOpsConfig_AuthNoneOnLoopback() {
	ops := config.OpsConfig{
		Addr: "127.0.0.1:9090",
		Auth: config.OpsAuthConfig{Type: "none"},
	}
	s.NoError(validateOpsConfig(ops))
}

func (s *AppOpsSuite) TestValidateOpsConfig_AuthNoneOnNonLoopbackRejects() {
	ops := config.OpsConfig{
		Addr: "0.0.0.0:9090",
		Auth: config.OpsAuthConfig{Type: "none"},
	}
	err := validateOpsConfig(ops)
	s.Require().Error(err)
	s.Contains(err.Error(), "loopback")
}

func (s *AppOpsSuite) TestValidateOpsConfig_EmptyAuthTypeOnNonLoopbackRejects() {
	ops := config.OpsConfig{
		Addr: "192.168.1.1:9090",
		Auth: config.OpsAuthConfig{Type: ""},
	}
	err := validateOpsConfig(ops)
	s.Require().Error(err)
	s.Contains(err.Error(), "loopback")
}

func (s *AppOpsSuite) TestValidateOpsConfig_BasicAuthNoUsersRejects() {
	ops := config.OpsConfig{
		Addr: "127.0.0.1:9090",
		Auth: config.OpsAuthConfig{Type: "basic", Users: nil},
	}
	err := validateOpsConfig(ops)
	s.Require().Error(err)
	s.Contains(err.Error(), "at least one")
}

func (s *AppOpsSuite) TestValidateOpsConfig_BasicAuthBadHashRejects() {
	ops := config.OpsConfig{
		Addr: "127.0.0.1:9090",
		Auth: config.OpsAuthConfig{
			Type: "basic",
			Users: []config.BasicAuthConfigUser{
				{Username: "admin", PasswordHash: "not-a-phc-hash"},
			},
		},
	}
	err := validateOpsConfig(ops)
	s.Require().Error(err)
	s.Contains(err.Error(), "argon2id")
}

func (s *AppOpsSuite) TestValidateOpsConfig_BasicAuthValidPassesOnLoopback() {
	hash := mustHashApp(s.T(), "correcthorse")
	ops := config.OpsConfig{
		Addr: "127.0.0.1:9090",
		Auth: config.OpsAuthConfig{
			Type: "basic",
			Users: []config.BasicAuthConfigUser{
				{Username: "admin", PasswordHash: hash},
			},
		},
	}
	s.NoError(validateOpsConfig(ops))
}

// CQ-5: basic auth over plaintext HTTP must be refused off-loopback —
// operator passwords and the mutating /ops/acl/reload would travel cleartext.
func (s *AppOpsSuite) TestValidateOpsConfig_BasicAuthNonLoopbackWithoutTLSRejects() {
	hash := mustHashApp(s.T(), "correcthorse")
	ops := config.OpsConfig{
		Addr: "0.0.0.0:9090",
		Auth: config.OpsAuthConfig{
			Type: "basic",
			Users: []config.BasicAuthConfigUser{
				{Username: "admin", PasswordHash: hash},
			},
		},
	}
	err := validateOpsConfig(ops)
	s.Require().Error(err)
	s.Contains(err.Error(), "tls")
}

func (s *AppOpsSuite) TestValidateOpsConfig_BasicAuthNonLoopbackWithTLSPasses() {
	hash := mustHashApp(s.T(), "correcthorse")
	ops := config.OpsConfig{
		Addr: "0.0.0.0:9090",
		Auth: config.OpsAuthConfig{
			Type: "basic",
			Users: []config.BasicAuthConfigUser{
				{Username: "admin", PasswordHash: hash},
			},
		},
		TLS: config.OpsTLSConfig{CertFile: "/etc/gmountie/ops.crt", KeyFile: "/etc/gmountie/ops.key"},
	}
	s.NoError(validateOpsConfig(ops))
}
