package gofuse

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// AppleXattrSuite covers the macOS com.apple.* <-> user.com.apple.* namespace
// remap. The pure mapping helpers are GOOS-independent so the round-trip is
// provable on Linux without a Mac or a real mount; the darwin-only wiring that
// calls them lives in applexattr_darwin.go.
type AppleXattrSuite struct {
	suite.Suite
}

func TestAppleXattrSuite(t *testing.T) {
	suite.Run(t, new(AppleXattrSuite))
}

func (s *AppleXattrSuite) TestToBackendMapsAppleNamespace() {
	s.Equal("user.com.apple.quarantine", appleXattrToBackend("com.apple.quarantine"))
	s.Equal("user.com.apple.metadata:kMDItemWhereFroms",
		appleXattrToBackend("com.apple.metadata:kMDItemWhereFroms"))
	s.Equal("user.com.apple.FinderInfo", appleXattrToBackend("com.apple.FinderInfo"))
}

func (s *AppleXattrSuite) TestToBackendLeavesOtherNamespacesUnchanged() {
	// A genuine user.* name a client sets directly must pass through verbatim,
	// or it would be double-stored and the security allowlist semantics change.
	s.Equal("user.test", appleXattrToBackend("user.test"))
	s.Equal("system.posix_acl_access", appleXattrToBackend("system.posix_acl_access"))
}

func (s *AppleXattrSuite) TestFromBackendReversesAppleNamespace() {
	s.Equal("com.apple.quarantine", appleXattrFromBackend("user.com.apple.quarantine"))
	s.Equal("com.apple.FinderInfo", appleXattrFromBackend("user.com.apple.FinderInfo"))
}

func (s *AppleXattrSuite) TestFromBackendLeavesPlainUserNamespace() {
	// Only the user.com.apple.* prefix is reversed; a plain user.* name stays.
	s.Equal("user.test", appleXattrFromBackend("user.test"))
	s.Equal("user.contract", appleXattrFromBackend("user.contract"))
}

func (s *AppleXattrSuite) TestRoundTripIsIdentity() {
	for _, name := range []string{
		"com.apple.quarantine",
		"com.apple.FinderInfo",
		"com.apple.ResourceFork",
		"com.apple.metadata:kMDItemWhereFroms",
	} {
		s.Equal(name, appleXattrFromBackend(appleXattrToBackend(name)),
			"round-trip must be identity for %q", name)
	}
}
