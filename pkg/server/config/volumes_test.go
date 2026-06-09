package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type VolumeConfigSuite struct{ suite.Suite }

func TestVolumeConfigSuite(t *testing.T) { suite.Run(t, new(VolumeConfigSuite)) }

func (s *VolumeConfigSuite) viperFrom(yaml string) *viper.Viper {
	v := viper.New()
	v.SetConfigType("yaml")
	s.Require().NoError(v.ReadConfig(strings.NewReader(yaml)))
	return v
}

func (s *VolumeConfigSuite) TestDefaultsToSquash() {
	v := s.viperFrom("name: photos\npath: /srv/photos\n")
	c, err := NewVolumeConfig(v)
	s.Require().NoError(err)
	s.Equal("photos", c.Name)
	s.Equal(MappingModeSquash, c.Mapping.Mode)
}

func (s *VolumeConfigSuite) TestParsesStaticTable() {
	v := s.viperFrom(`
name: appliance
path: /srv/app
mapping:
  mode: static
  users:
    alice: {uid: 1001, gid: 1001, groups: [developers]}
  groups:
    developers: 2000
`)
	c, err := NewVolumeConfig(v)
	s.Require().NoError(err)
	s.Equal(MappingModeStatic, c.Mapping.Mode)
	s.Equal(uint32(1001), c.Mapping.Users["alice"].Uid)
	s.Equal([]string{"developers"}, c.Mapping.Users["alice"].Groups)
	s.Equal(uint32(2000), c.Mapping.Groups["developers"])
}

func (s *VolumeConfigSuite) TestParsesPassthroughRootSquash() {
	v := s.viperFrom("name: lan\npath: /srv/lan\nmapping:\n  mode: passthrough\n  root_squash: false\n")
	c, err := NewVolumeConfig(v)
	s.Require().NoError(err)
	s.Equal(MappingModePassthrough, c.Mapping.Mode)
	s.Require().NotNil(c.Mapping.RootSquash)
	s.False(*c.Mapping.RootSquash)
}

func (s *VolumeConfigSuite) TestParsesSystemAdminGroups() {
	v := s.viperFrom(`
name: team
path: /srv/team
mapping:
  mode: system
  admin_groups:
    dac_override: [wheel]
    dac_read_search: [backup]
`)
	c, err := NewVolumeConfig(v)
	s.Require().NoError(err)
	s.Equal(MappingModeSystem, c.Mapping.Mode)
	s.Equal([]string{"wheel"}, c.Mapping.AdminGroups["dac_override"])
	s.Equal([]string{"backup"}, c.Mapping.AdminGroups["dac_read_search"])
}

// TestMappingUnmarshalErrorSurfaces pins CQ-4: a type-mismatched mapping
// block must fail config parsing instead of silently part-decoding into a
// wrong identity mapping.
func (s *VolumeConfigSuite) TestMappingUnmarshalErrorSurfaces() {
	v := s.viperFrom("name: bad\npath: /srv/bad\nmapping:\n  mode: squash\n  uid: alice\n")
	_, err := NewVolumeConfig(v)
	s.Require().Error(err, "non-numeric uid must fail the mapping unmarshal")
	s.Contains(err.Error(), "mapping")
}
