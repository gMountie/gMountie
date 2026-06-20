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
	opts := macOSMountOptions("photos", providerFuseT, 1<<20)
	s.Contains(opts, "volname=photos")
	s.NotContains(opts, "local") // FUSE-T rejects unknown opts
	// FUSE-T accepts libfuse-style max_write; large value avoids write
	// fragmentation (each fragment crosses cgo).
	s.Contains(opts, "max_write=1048576")
}

func (s *MacProviderSuite) TestOptionsIncludeLocalForMacFUSE() {
	opts := macOSMountOptions("photos", providerMacFUSE, 1<<20)
	s.Contains(opts, "local")
	s.Contains(opts, "volname=photos")
	// macFUSE sizes I/O via iosize, not libfuse max_write.
	s.Contains(opts, "iosize=1048576")
}

func (s *MacProviderSuite) TestLinuxCgofuseOptionsSetMaxWrite() {
	// On Linux, cgofuse mounts via libfuse: the macOS options (volname/local/
	// noappledouble) are rejected, but big_writes + max_write are required to
	// stop libfuse fragmenting writes into tiny cgo-crossing ops.
	opts := linuxCgofuseOptions(1 << 20)
	s.Contains(opts, "big_writes")
	s.Contains(opts, "max_write=1048576")
	// No negotiated value -> defer to libfuse defaults (no -o options).
	s.Empty(linuxCgofuseOptions(0))
}

func (s *MacProviderSuite) TestPathExists() {
	// A path that cannot exist returns false; the test binary itself exists.
	s.False(pathExists("/nonexistent/gmountie/probe/path"))
	exe, err := os.Executable()
	s.Require().NoError(err)
	s.True(pathExists(exe))
}
