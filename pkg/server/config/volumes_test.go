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
	c := NewVolumeConfig(v)
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
	c := NewVolumeConfig(v)
	s.Equal(MappingModeStatic, c.Mapping.Mode)
	s.Equal(uint32(1001), c.Mapping.Users["alice"].Uid)
	s.Equal([]string{"developers"}, c.Mapping.Users["alice"].Groups)
	s.Equal(uint32(2000), c.Mapping.Groups["developers"])
}

func (s *VolumeConfigSuite) TestParsesPassthroughRootSquash() {
	v := s.viperFrom("name: lan\npath: /srv/lan\nmapping:\n  mode: passthrough\n  root_squash: false\n")
	c := NewVolumeConfig(v)
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
	c := NewVolumeConfig(v)
	s.Equal(MappingModeSystem, c.Mapping.Mode)
	s.Equal([]string{"wheel"}, c.Mapping.AdminGroups["dac_override"])
	s.Equal([]string{"backup"}, c.Mapping.AdminGroups["dac_read_search"])
}
