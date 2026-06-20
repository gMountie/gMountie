package mount

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MacProviderSuite struct{ suite.Suite }

func TestMacProviderSuite(t *testing.T) { suite.Run(t, new(MacProviderSuite)) }

func (s *MacProviderSuite) TestAutoPrefersMacFUSEWhenBothPresent() {
	p, err := detectProvider(providerAuto, func(string) bool { return true })
	s.NoError(err)
	s.Equal(providerMacFUSE, p)
}

func (s *MacProviderSuite) TestAutoFallsBackToFuseT() {
	exists := func(path string) bool { return path == fuseTLibPath }
	p, err := detectProvider(providerAuto, exists)
	s.NoError(err)
	s.Equal(providerFuseT, p)
}

func (s *MacProviderSuite) TestAutoErrorsWhenNeitherPresent() {
	_, err := detectProvider(providerAuto, func(string) bool { return false })
	s.Error(err)
}

func (s *MacProviderSuite) TestExplicitOverrideHonored() {
	p, err := detectProvider(providerFuseT, func(string) bool { return false })
	s.NoError(err)
	s.Equal(providerFuseT, p)
}

func (s *MacProviderSuite) TestOptionsIncludeVolnameAlways() {
	opts := macOSMountOptions("photos", providerFuseT)
	s.Contains(opts, "volname=photos")
	s.NotContains(opts, "local") // FUSE-T rejects unknown opts
}

func (s *MacProviderSuite) TestOptionsIncludeLocalForMacFUSE() {
	opts := macOSMountOptions("photos", providerMacFUSE)
	s.Contains(opts, "local")
	s.Contains(opts, "volname=photos")
}
