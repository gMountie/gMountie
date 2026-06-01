package fs

import (
	"os"
	"path/filepath"
	"testing"

	clientconfig "go.gmountie.dev/gmountie/pkg/client/config"
	commonconfig "go.gmountie.dev/gmountie/pkg/common/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type ProfileResolveSuite struct{ suite.Suite }

func TestProfileResolveSuite(t *testing.T) { suite.Run(t, new(ProfileResolveSuite)) }

// A profile file resolves through the profiles dir + ParseConfig into a valid
// client config carrying the volume — the contract the mount command relies on.
func (s *ProfileResolveSuite) TestProfileResolvesToClientConfig() {
	cfgHome := s.T().TempDir()
	s.T().Setenv("XDG_CONFIG_HOME", cfgHome)
	pdir := filepath.Join(cfgHome, "gmountie", "profiles")
	s.Require().NoError(os.MkdirAll(pdir, 0o755))
	profile := "server:\n  address: 127.0.0.1\n  port: 9449\nauth:\n  type: basic\n  username: demo\n  password: demo\nmount:\n  type: single\n  volume: shared\n"
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "work.yaml"), []byte(profile), 0o600))

	s.Require().NoError(commonconfig.ValidateProfileName("work"))
	path := commonconfig.GetProfilePath("work")

	v := viper.New()
	v.SetConfigFile(path)
	s.Require().NoError(v.ReadInConfig())
	cfg, err := clientconfig.ParseConfig(v)
	s.Require().NoError(err)
	sm, ok := cfg.Mount.(*clientconfig.SingleMountConfig)
	s.Require().True(ok)
	s.Equal("shared", sm.Volume)
}
