package commands

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ConfigShowSuite struct{ suite.Suite }

func TestConfigShowSuite(t *testing.T) { suite.Run(t, new(ConfigShowSuite)) }

func (s *ConfigShowSuite) TestRedactsSecrets() {
	in := "auth:\n  username: admin\n  password: supersecret\nserver:\n  address: 1.2.3.4\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "supersecret")
	s.Contains(out, "REDACTED")
	s.Contains(out, "admin")
	s.Contains(out, "1.2.3.4")
}

func (s *ConfigShowSuite) TestRedactsPasswordHash() {
	in := "auth:\n  users:\n    - username: admin\n      password_hash: $argon2id$abc\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "argon2id$abc")
	s.Contains(out, "REDACTED")
}
