package config

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type BasicAuthConfigTestSuite struct {
	suite.Suite
}

// newViperFromMap builds a minimal viper suitable for NewBasicAuthConfig.
func newViperFromMap(m map[string]interface{}) *viper.Viper {
	v := viper.New()
	for k, val := range m {
		v.Set(k, val)
	}
	return v
}

func (s *BasicAuthConfigTestSuite) TestRejectsCleartextPasswordHash() {
	// A plaintext password must be rejected with a helpful error pointing
	// the operator to 'gmountie genpass'.
	v := newViperFromMap(map[string]interface{}{
		"type": "basic",
		"users": []interface{}{
			map[string]interface{}{
				"username":      "admin",
				"password_hash": "hunter2",
			},
		},
	})

	_, err := NewBasicAuthConfig(v)
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "gmountie genpass")
}

func (s *BasicAuthConfigTestSuite) TestAcceptsValidPHCHash() {
	// A valid $argon2id$ PHC string must parse without error.
	// This is a known-good PHC for "testpass" (salt is deterministic for
	// this test — we use a real Hash call via the passhash package tests;
	// here we embed a pre-generated string so the test is fast and offline).
	validPHC := "$argon2id$v=19$m=65536,t=3,p=4$AAAAAAAAAAAAAAAAAAAAAA$5u/nk9QnWzMnHQ7LHnzNBwZmJvH2tXG7dBbJGT7C7Ho"
	v := newViperFromMap(map[string]interface{}{
		"type": "basic",
		"users": []interface{}{
			map[string]interface{}{
				"username":      "admin",
				"password_hash": validPHC,
			},
		},
	})

	cfg, err := NewBasicAuthConfig(v)
	s.Require().NoError(err)
	s.Require().Len(cfg.Users, 1)
	s.Assert().Equal("admin", cfg.Users[0].Username)
	s.Assert().Equal(validPHC, cfg.Users[0].PasswordHash)
}

func TestBasicAuthConfigTestSuite(t *testing.T) {
	suite.Run(t, new(BasicAuthConfigTestSuite))
}
