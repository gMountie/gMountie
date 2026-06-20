package mount

import (
	"os"
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

func (s *MacProviderSuite) TestLinuxCgofuseOptionsAreEmpty() {
	// On Linux, cgofuse mounts via libfuse, which rejects the macOS options
	// (volname/local/noappledouble) — so the Linux path passes none. Regression
	// guard for the "fuse: unknown option volname" mount failure on linux+cgofuse.
	s.Empty(linuxCgofuseOptions("photos"))
}

func (s *MacProviderSuite) TestPathExists() {
	// A path that cannot exist returns false; the test binary itself exists.
	s.False(pathExists("/nonexistent/gmountie/probe/path"))
	exe, err := os.Executable()
	s.Require().NoError(err)
	s.True(pathExists(exe))
}
