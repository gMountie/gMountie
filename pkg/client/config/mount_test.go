package config

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MountConfigTestSuite struct {
	suite.Suite
}

// Test successful parsing of "single" mount configuration
func (s *MountConfigTestSuite) TestParse_SingleMount() {
	conf := `
mount:
  type: single
  path: /mnt/test
  volume: testvol
` + minimalServerConfig
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(MountTypeSingle, result.Mount.GetType())

	singleMount, ok := result.Mount.(*SingleMountConfig)
	s.Require().True(ok)
	s.Assert().Equal("/mnt/test", singleMount.Path)
	s.Assert().Equal("testvol", singleMount.Volume)
}

// Test error cases for invalid mount type — includes "vfs" which is no longer a valid type
func (s *MountConfigTestSuite) TestParse_InvalidMountType() {
	for _, typ := range []string{"invalid", "vfs"} {
		conf := `
mount:
  type: ` + typ + `
  path: /mnt/test
` + minimalServerConfig
		_, err := LoadConfigFromString(conf)
		s.Require().Errorf(err, "expected error for mount type %q", typ)
		s.Assert().Contains(err.Error(), "invalid mount type")
	}
}

// Test error cases for missing required fields
func (s *MountConfigTestSuite) TestParse_MissingRequiredFields() {
	// Missing path
	conf := `
mount:
  type: single
  volume: testvol
` + minimalServerConfig
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

func TestMountConfigTestSuite(t *testing.T) {
	suite.Run(t, new(MountConfigTestSuite))
}
