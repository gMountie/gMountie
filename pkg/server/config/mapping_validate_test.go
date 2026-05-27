package config

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MappingValidateSuite struct{ suite.Suite }

func TestMappingValidateSuite(t *testing.T) { suite.Run(t, new(MappingValidateSuite)) }

func (s *MappingValidateSuite) TestSystemRequiresAuth() {
	err := ValidateMapping(MappingModeSystem, AuthConfigTypeNone)
	s.Require().Error(err)
	s.Contains(err.Error(), "auth")
}

func (s *MappingValidateSuite) TestStaticRequiresAuth() {
	s.Require().Error(ValidateMapping(MappingModeStatic, AuthConfigTypeNone))
}

func (s *MappingValidateSuite) TestSquashAllowsNoAuth() {
	s.Require().NoError(ValidateMapping(MappingModeSquash, AuthConfigTypeNone))
}

func (s *MappingValidateSuite) TestPassthroughAllowsNoAuth() {
	s.Require().NoError(ValidateMapping(MappingModePassthrough, AuthConfigTypeNone))
}
