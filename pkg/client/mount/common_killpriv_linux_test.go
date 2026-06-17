//go:build linux

package mount

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/config"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

// CreateMountOptionsSuite tests CAP_HANDLE_KILLPRIV_V2 wiring in
// createMountOptions. It is Linux-only: the capability constant lives in
// go-fuse's types_linux.go, and the cap is only advertised on Linux (see
// enableKillPrivCap in killpriv_linux.go / killpriv_other.go).
type CreateMountOptionsSuite struct{ suite.Suite }

func (s *CreateMountOptionsSuite) baseCfg() *config.FUSEConfig {
	return &config.FUSEConfig{
		MaxWriteBytes:  config.DefaultFUSEMaxWriteBytes,
		MaxBackground:  config.DefaultFUSEMaxBackground,
		WritebackCache: false,
		AttrTimeout:    config.DefaultFUSEAttrTimeout,
		EntryTimeout:   config.DefaultFUSEEntryTimeout,
	}
}

func (s *CreateMountOptionsSuite) TestHandleKillPrivOnSetsCap() {
	cfg := s.baseCfg()
	cfg.HandleKillPriv = true
	opts := createMountOptions("127.0.0.1:9449", "vol", cfg, config.DefaultFUSEMaxWriteBytes)
	s.NotZero(opts.ExtraCapabilities&fuse.CAP_HANDLE_KILLPRIV_V2,
		"HandleKillPriv=true must set CAP_HANDLE_KILLPRIV_V2 in ExtraCapabilities")
}

func (s *CreateMountOptionsSuite) TestHandleKillPrivOffLeavesCapUnset() {
	cfg := s.baseCfg()
	cfg.HandleKillPriv = false
	opts := createMountOptions("127.0.0.1:9449", "vol", cfg, config.DefaultFUSEMaxWriteBytes)
	s.Zero(opts.ExtraCapabilities&fuse.CAP_HANDLE_KILLPRIV_V2,
		"HandleKillPriv=false must leave CAP_HANDLE_KILLPRIV_V2 unset")
}

func TestCreateMountOptionsSuite(t *testing.T) {
	suite.Run(t, new(CreateMountOptionsSuite))
}
