package fs

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	clientconfig "go.gmountie.dev/gmountie/pkg/client/config"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/mount"
	commonconfig "go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

// ProfileMountSuite verifies the headline profile feature end-to-end: a profile
// YAML file (resolved via XDG_CONFIG_HOME) drives grpc.NewClientForVolume +
// mount.NewSingleVolumeMounter to produce a real FUSE mount, asserts directory
// I/O, then unmounts cleanly.
//
// Like the rest of the test/e2e/fs suite it performs a real FUSE mount, so it
// runs on the CI runners and the kubevirt VM (which have /dev/fuse) and is not
// run in FUSE-less sandboxes. It deliberately does not skip on mount failure —
// a skip would mask a real regression in CI.
type ProfileMountSuite struct {
	suite.Suite
	appCtx *utils.AppTestingContext
	volume *utils.TestVolume
}

func (s *ProfileMountSuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithTCPTransport(),
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(true),
	)
	if err != nil {
		s.T().Fatal(err)
	}
	if err := ctx.Start(); err != nil {
		s.T().Fatal(err)
	}
	s.appCtx = ctx
	s.volume = ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.Require().GreaterOrEqual(len(s.volume.GeneratedFiles), 1)
}

func (s *ProfileMountSuite) TearDownSuite() {
	if s.appCtx == nil {
		return
	}
	if err := s.appCtx.Close(); err != nil {
		s.T().Logf("TearDownSuite: app context close error: %v", err)
	}
}

// TestMountViaProfile writes a profile YAML that points at the in-process test
// server, resolves it through the same path the mount command uses, builds a
// client + mounter from the resolved config, mounts the volume, asserts basic
// I/O, and unmounts.
func (s *ProfileMountSuite) TestMountViaProfile() {
	// --- 1. Wire a temporary XDG_CONFIG_HOME so GetProfilePath resolves here ---
	cfgHome := s.T().TempDir()
	s.T().Setenv("XDG_CONFIG_HOME", cfgHome)
	pdir := filepath.Join(cfgHome, "gmountie", "profiles")
	s.Require().NoError(os.MkdirAll(pdir, 0o755))

	// --- 2. Parse the test server's TCP endpoint into host + port ---
	endpoint := s.appCtx.GetEndpoint() // "127.0.0.1:<port>"
	host, port, err := net.SplitHostPort(endpoint)
	s.Require().NoError(err, "split test-server endpoint %q", endpoint)

	// --- 3. Write the profile YAML ---
	// server.tls.verify: insecure — the test server uses an ephemeral self-signed
	// cert; skipping verification lets the config-built client dial it without
	// needing to install the cert or use a fingerprint pin.
	profileYAML := fmt.Sprintf(`server:
  address: %s
  port: %s
  tls:
    verify: insecure
auth:
  type: basic
  username: test
  password: test
mount:
  type: single
  volume: %s
`, host, port, s.volume.Name)

	const profileName = "e2e-profile-mount"
	s.Require().NoError(commonconfig.ValidateProfileName(profileName))
	profilePath := commonconfig.GetProfilePath(profileName)
	s.Require().NoError(os.WriteFile(profilePath, []byte(profileYAML), 0o600))

	// --- 4. Resolve profile → client config (same path as mount command) ---
	v := viper.New()
	v.SetConfigFile(profilePath)
	s.Require().NoError(v.ReadInConfig())
	cfg, err := clientconfig.ParseConfig(v)
	s.Require().NoError(err, "ParseConfig from profile")

	sm, ok := cfg.Mount.(*clientconfig.SingleMountConfig)
	s.Require().True(ok, "mount config must be SingleMountConfig")
	s.Require().Equal(s.volume.Name, sm.Volume)

	// --- 5. Build client + mounter exactly as mount command does ---
	// Defers are registered before Mount so cleanup runs even on mount failure.
	c, _, err := grpcclient.NewClientForVolume(cfg, s.volume.Name)
	s.Require().NoError(err, "NewClientForVolume from profile config")
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			s.T().Logf("client Close: %v", closeErr)
		}
	}()

	mounter := mount.NewSingleVolumeMounter(c, cfg.FUSE, *cfg.Cache, *cfg.WAL, false)
	defer func() {
		if closeErr := mounter.Close(); closeErr != nil {
			s.T().Logf("mounter Close: %v", closeErr)
		}
	}()

	// --- 6. Mount for real (CI runners have /dev/fuse; this suite is not run
	// in FUSE-less sandboxes). Do NOT skip on mount failure — a skip would mask
	// a real regression in CI. ---
	mountPath := s.volume.GetMountPath()
	s.Require().NoError(mounter.Mount(s.volume.Name, mountPath))

	// --- 7. Assert basic I/O through the mount ---
	testFS := os.DirFS(mountPath)
	s.Require().NoError(
		fstest.TestFS(testFS, s.volume.GeneratedFiles...),
		"fstest.TestFS through profile-mounted volume",
	)
}

func TestProfileMountSuite(t *testing.T) {
	suite.Run(t, new(ProfileMountSuite))
}
