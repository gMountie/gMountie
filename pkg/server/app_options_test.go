package server

import (
	"testing"

	commonconfig "go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/watermark"

	"github.com/stretchr/testify/suite"
)

// fakeWMStore is a minimal in-memory watermark.Store for option-seam tests.
type fakeWMStore struct{}

func (f *fakeWMStore) Get(_ watermark.Key) (watermark.Record, error) { return watermark.Record{}, nil }
func (f *fakeWMStore) Advance(_ watermark.Key, _ uint64) error       { return nil }
func (f *fakeWMStore) RevokeGen(_ watermark.Key, _ uint64) error     { return nil }
func (f *fakeWMStore) NextGen(_ watermark.Key) (uint64, error)       { return 1, nil }
func (f *fakeWMStore) Close() error                                  { return nil }

// testConfig builds a minimal server config suitable for NewServerAppContext
// in tests. TLS is disabled and bound to loopback only.
func testConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		Server: &config.ServerConfig{
			Address: "127.0.0.1",
			Port:    0,
			TLS:     config.TLSConfig{Disabled: true},
			Metrics: false,
			Keepalive: config.ServerKeepaliveConfig{
				Time:                config.DefaultKeepaliveTime,
				Timeout:             config.DefaultKeepaliveTimeout,
				MinTime:             config.DefaultKeepaliveMinTime,
				PermitWithoutStream: config.DefaultKeepalivePermitWithoutStream,
			},
		},
		Auth: &config.BasicAuthConfig{
			AuthConfigBase: commonconfig.AuthConfigBase{Type: commonconfig.AuthConfigTypeBasic},
			Users: []config.BasicAuthConfigUser{
				{Username: "admin", PasswordHash: mustHashApp(t, "admin")},
			},
		},
		Volumes: []*config.VolumeConfig{},
	}
}

// AppOptionsSuite tests the AppContextOption / WithWatermarkStore seam.
type AppOptionsSuite struct {
	suite.Suite
}

// TestWithWatermarkStoreOverridesDefault verifies that a store passed via
// WithWatermarkStore is exactly the one stored on AppContext.Watermark.
func (s *AppOptionsSuite) TestWithWatermarkStoreOverridesDefault() {
	fake := &fakeWMStore{}
	appCtx, err := NewServerAppContext(testConfig(s.T()), WithWatermarkStore(fake))
	s.Require().NoError(err)
	s.Same(fake, appCtx.Watermark)
}

// TestDefaultWatermarkStoreIsBBolt verifies that when no option is passed a
// non-nil (bbolt) store is opened.
func (s *AppOptionsSuite) TestDefaultWatermarkStoreIsBBolt() {
	// Redirect XDG_STATE_HOME to a temp dir to avoid polluting ~/.local/state.
	s.T().Setenv("XDG_STATE_HOME", s.T().TempDir())
	appCtx, err := NewServerAppContext(testConfig(s.T()))
	s.Require().NoError(err)
	s.NotNil(appCtx.Watermark)
	// Clean up the bbolt file the default path opened.
	_ = appCtx.Watermark.Close()
}

func TestAppOptionsSuite(t *testing.T) {
	suite.Run(t, new(AppOptionsSuite))
}
