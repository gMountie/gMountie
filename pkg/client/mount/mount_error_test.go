package mount

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type WrapMountErrorTestSuite struct {
	suite.Suite
	savedGOOS string
}

func (s *WrapMountErrorTestSuite) SetupTest() {
	s.savedGOOS = currentGOOS
}

func (s *WrapMountErrorTestSuite) TearDownTest() {
	currentGOOS = s.savedGOOS
}

func (s *WrapMountErrorTestSuite) TestNilPassesThrough() {
	s.Nil(wrapMountError(nil))
}

func (s *WrapMountErrorTestSuite) TestLinuxNeverWraps() {
	currentGOOS = "linux"
	// Even an error that LOOKS like missing FUSE on darwin must pass through on Linux.
	in := errors.New("open /dev/osxfuse0: no such file or directory")
	out := wrapMountError(in)
	s.Same(in, out, "Linux must return the same error pointer unchanged")
}

func (s *WrapMountErrorTestSuite) TestDarwinUnrelatedErrorPassesThrough() {
	currentGOOS = "darwin"
	in := errors.New("network unreachable")
	out := wrapMountError(in)
	s.Same(in, out, "darwin with unrelated error must pass through")
}

func (s *WrapMountErrorTestSuite) TestDarwinMissingProviderWraps() {
	currentGOOS = "darwin"

	cases := []string{
		"exec: \"mount_macfuse\": executable file not found in $PATH",
		"open /dev/macfuse0: no such file or directory",
		"open /dev/osxfuse0: no such file or directory",
		"MACFUSE failed to load",        // ensures case-insensitive matching
		"fork/exec /usr/local/bin/mount_osxfuse: no such file or directory",
	}

	for _, msg := range cases {
		s.Run(msg, func() {
			in := errors.New(msg)
			out := wrapMountError(in)

			s.Require().NotNil(out)
			s.NotSame(in, out, "matching error should be wrapped, not returned as-is")

			text := out.Error()
			s.Contains(text, "FUSE driver missing",
				"wrapper must include the canonical hint phrase")
			s.Contains(text, "macfuse.io",
				"wrapper must point at macFUSE")
			s.Contains(text, "fuse-t.org",
				"wrapper must point at FUSE-T")
			s.Contains(text, msg,
				"wrapper must preserve the original error text")
			s.True(errors.Is(out, in) || strings.Contains(text, msg),
				"original error should be discoverable")
		})
	}
}

func TestWrapMountErrorTestSuite(t *testing.T) {
	suite.Run(t, new(WrapMountErrorTestSuite))
}
