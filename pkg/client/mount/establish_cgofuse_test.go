//go:build darwin || cgofuse

package mount

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type FuseTBackendSuite struct{ suite.Suite }

func TestFuseTBackendSuite(t *testing.T) { suite.Run(t, new(FuseTBackendSuite)) }

var (
	moduleInstalled = func(string) bool { return true }
	moduleMissing   = func(string) bool { return false }
)

func (s *FuseTBackendSuite) TestAutoPrefersFskitWhenInstalled() {
	b, fallback, err := resolveFuseTBackend("auto", moduleInstalled)
	s.Require().NoError(err)
	s.Equal("fskit", b)
	s.True(fallback, "auto must allow NFS fallback if the fskit mount fails")
}

func (s *FuseTBackendSuite) TestAutoUsesNfsWhenModuleMissing() {
	b, fallback, err := resolveFuseTBackend("auto", moduleMissing)
	s.Require().NoError(err)
	s.Equal("nfs", b)
	s.False(fallback)
}

func (s *FuseTBackendSuite) TestEmptyDefaultsToAutoBehavior() {
	b, fallback, err := resolveFuseTBackend("", moduleInstalled)
	s.Require().NoError(err)
	s.Equal("fskit", b)
	s.True(fallback)
}

func (s *FuseTBackendSuite) TestForceFskitErrorsWhenModuleMissing() {
	_, _, err := resolveFuseTBackend("fskit", moduleMissing)
	s.Require().Error(err, "explicit fskit must fail clearly when the module is absent")
}

func (s *FuseTBackendSuite) TestForceFskitNoFallbackWhenInstalled() {
	b, fallback, err := resolveFuseTBackend("fskit", moduleInstalled)
	s.Require().NoError(err)
	s.Equal("fskit", b)
	s.False(fallback, "explicit fskit must not silently fall back to NFS")
}

func (s *FuseTBackendSuite) TestForceNfs() {
	b, fallback, err := resolveFuseTBackend("nfs", moduleInstalled)
	s.Require().NoError(err)
	s.Equal("nfs", b)
	s.False(fallback)
}
