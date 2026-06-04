//go:build linux

package commands

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/suite"
)

type LsSuite struct{ suite.Suite }

func TestLsSuite(t *testing.T) { suite.Run(t, new(LsSuite)) }

func (s *LsSuite) TestRenderVolumes() {
	var out bytes.Buffer
	renderVolumes(&out, []string{"shared", "backups"})
	s.Contains(out.String(), "shared")
	s.Contains(out.String(), "backups")
}

func (s *LsSuite) TestRenderEmpty() {
	var out bytes.Buffer
	renderVolumes(&out, nil)
	s.Contains(out.String(), "no volumes")
}

// newLsRoot builds a test root with the persistent --config flag and lsCmd
// attached, mirroring the real rootCmd wiring tests rely on.
func (s *LsSuite) newLsRoot() (*cobra.Command, *bytes.Buffer) {
	root := &cobra.Command{Use: "root"}
	root.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file path")
	root.AddCommand(lsCmd)
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetIn(bytes.NewReader(nil))
	return root, buf
}

// resetLsState clears the package-level flag vars and cobra Changed state that
// ls shares with mount, so credential/auth tests don't leak into each other.
func (s *LsSuite) resetLsState() {
	authType = "basic"
	username = ""
	password = ""
	configFile = ""
	profileName = ""
	credentialsFile = ""
	lsCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) { f.Changed = false })
	lsCmd.Flags().VisitAll(func(f *pflag.Flag) { f.Changed = false })
}

// TestLsCmd_CredentialEnv_CertOnlyAuth proves the resolver path: with
// $GMOUNTIE_CREDENTIALS set and NO host arg and NO -u, ls applies the credential
// (auth.type=mtls) so resolveAuth does not demand a username, then reaches the
// pre-session ListVolumes call (which fails at TLS/dial with the opaque test
// PEM — proving it got past auth resolution, not blocked before it).
func (s *LsSuite) TestLsCmd_CredentialEnv_CertOnlyAuth() {
	s.resetLsState()
	defer s.resetLsState()
	s.T().Setenv(credentialsEnvVar, credBlob("192.168.11.11", "9449"))

	root, _ := s.newLsRoot()
	root.SetArgs([]string{"ls"})
	err := root.Execute()

	s.Require().Error(err)
	s.Assert().NotContains(err.Error(), "username is required")
	s.Assert().NotContains(err.Error(), "invalid auth type")
}

func (s *LsSuite) TestLsCmd_ProfileAndConfigConflict() {
	profileName, configFile = "", ""
	defer func() { profileName, configFile = "", "" }()

	root := &cobra.Command{Use: "root"}
	root.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file path")
	root.AddCommand(lsCmd)
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"ls", "--profile", "work", "--config", "/tmp/x.yaml"})

	err := root.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "one of --profile or --config")
}
