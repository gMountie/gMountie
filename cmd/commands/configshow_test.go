package commands

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/suite"
)

type ConfigShowSuite struct{ suite.Suite }

func TestConfigShowSuite(t *testing.T) { suite.Run(t, new(ConfigShowSuite)) }

func (s *ConfigShowSuite) TestRedactsSecrets() {
	in := "auth:\n  username: admin\n  password: supersecret\nserver:\n  address: 1.2.3.4\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "supersecret")
	s.Contains(out, "REDACTED")
	s.Contains(out, "admin")
	s.Contains(out, "1.2.3.4")
}

func (s *ConfigShowSuite) TestRedactsPasswordHash() {
	in := "auth:\n  users:\n    - username: admin\n      password_hash: $argon2id$abc\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "argon2id$abc")
	s.Contains(out, "REDACTED")
}

// TestRedactsSecretAsFirstListItemKey guards a leak surfaced by --effective:
// when a secret is the first key of a YAML sequence item it renders with a
// "- " marker (`- password_hash: ...`), which a whitespace-anchored redactor
// misses. Both server users[] and any re-marshalled list can produce this.
func (s *ConfigShowSuite) TestRedactsSecretAsFirstListItemKey() {
	in := "auth:\n" +
		"  users:\n" +
		"    - password_hash: $argon2id$SECRETDIGEST\n" +
		"      username: admin\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "SECRETDIGEST")
	s.Contains(out, "REDACTED")
	s.Contains(out, "username: admin") // the list item's other fields survive
}

// TestRedactsBlockScalarKeyPEM is the real hole: an inline mTLS private key is
// written as a YAML block scalar, so its secret bytes live on indented lines
// after `key_pem: |`, which the single-line redactor never touched.
func (s *ConfigShowSuite) TestRedactsBlockScalarKeyPEM() {
	in := "server:\n" +
		"  address: 1.2.3.4\n" +
		"  tls:\n" +
		"    ca_pem: |\n" +
		"      -----BEGIN CERTIFICATE-----\n" +
		"      PUBLICCABYTES\n" +
		"      -----END CERTIFICATE-----\n" +
		"    key_pem: |\n" +
		"      -----BEGIN PRIVATE KEY-----\n" +
		"      SUPERSECRETKEYBYTES\n" +
		"      -----END PRIVATE KEY-----\n"
	out := redactConfigYAML(in)
	// The private key and its PEM envelope must be gone.
	s.NotContains(out, "SUPERSECRETKEYBYTES")
	s.NotContains(out, "BEGIN PRIVATE KEY")
	s.Contains(out, "REDACTED")
	// Public material and surrounding structure must be preserved.
	s.Contains(out, "PUBLICCABYTES")
	s.Contains(out, "1.2.3.4")
	s.Contains(out, "key_pem:")
}

// TestBlockScalarRedactionStopsAtDedent ensures redacting a block-scalar
// secret consumes only its own indented body and leaves the following sibling
// keys and their values intact.
func (s *ConfigShowSuite) TestBlockScalarRedactionStopsAtDedent() {
	in := "server:\n" +
		"  tls:\n" +
		"    key_pem: |\n" +
		"      -----BEGIN PRIVATE KEY-----\n" +
		"      SUPERSECRETKEYBYTES\n" +
		"      -----END PRIVATE KEY-----\n" +
		"    cert_pem: PUBLICCERT\n" +
		"  address: 1.2.3.4\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "SUPERSECRETKEYBYTES")
	// The sibling cert_pem and the dedented address must survive untouched.
	s.Contains(out, "cert_pem: PUBLICCERT")
	s.Contains(out, "address: 1.2.3.4")
}

// TestRedactsBlockScalarWithTrailingComment guards a bypass: YAML allows a
// comment after the block-scalar indicator (`key_pem: | # note`). If that line
// isn't recognized as a block header, the indented secret body leaks.
func (s *ConfigShowSuite) TestRedactsBlockScalarWithTrailingComment() {
	in := "server:\n" +
		"  tls:\n" +
		"    key_pem: |   # operator's mTLS key\n" +
		"      -----BEGIN PRIVATE KEY-----\n" +
		"      STILLLEAKSBYTES\n" +
		"      -----END PRIVATE KEY-----\n" +
		"  address: 1.2.3.4\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "STILLLEAKSBYTES")
	s.NotContains(out, "BEGIN PRIVATE KEY")
	s.Contains(out, "address: 1.2.3.4")
}

// TestRedactsInlineKeyPEM covers a key_pem given as a single-line scalar.
func (s *ConfigShowSuite) TestRedactsInlineKeyPEM() {
	in := "server:\n  tls:\n    key_pem: secretinline\n  address: 1.2.3.4\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "secretinline")
	s.Contains(out, "REDACTED")
	s.Contains(out, "1.2.3.4")
}

func (s *ConfigShowSuite) TestConfigShow_EffectiveProfileWithoutInlinePassword() {
	profileName = "work"
	defer func() { profileName = "" }()
	configFile = ""
	configShowEffective = true
	defer func() { configShowEffective = false }()

	cfgHome := s.T().TempDir()
	s.T().Setenv("XDG_CONFIG_HOME", cfgHome)
	pdir := filepath.Join(cfgHome, "gmountie", "profiles")
	s.Require().NoError(os.MkdirAll(pdir, 0o755))
	// No inline password — auth.password_command supplies it at mount time.
	// Use "pass show work" so the command text is distinct from any possible
	// command output; the command is never executed by config show.
	profile := "server:\n  address: work.example.com\n  port: 9449\nauth:\n  type: basic\n  username: admin\n  password_command: \"pass show gmountie/work\"\nmount:\n  type: single\n  volume: shared\n"
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "work.yaml"), []byte(profile), 0o600))

	path, err := resolveConfigShowPath()
	s.Require().NoError(err)
	out, err := renderEffectiveConfig(path, "client")
	s.Require().NoError(err) // must NOT fail on "password is required"
	s.Contains(out, "address: work.example.com")
	s.Contains(out, "username: admin")
	// password_command is a path/command, not a secret — it renders verbatim.
	s.Contains(out, "password_command:")
	s.Contains(out, "pass show gmountie/work")
	s.NotContains(out, "password_command: REDACTED")
}

// TestConfigShow_ProfileAndConfigConflict proves that --profile and --config
// together produce a clear error, matching the guard on mount and ls.
func (s *ConfigShowSuite) TestConfigShow_ProfileAndConfigConflict() {
	profileName, configFile = "", ""
	defer func() { profileName, configFile = "", "" }()
	root := &cobra.Command{Use: "root"}
	root.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file path")
	root.AddCommand(configCmd)
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"config", "show", "--profile", "work", "--config", "/tmp/x.yaml"})
	err := root.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "one of --profile or --config")
}

// TestRedactsRenewToken proves that an inline renew.token bearer secret is
// masked by config show. token_file is a path, not a credential, and must
// NOT be redacted — this test enforces both properties together.
func (s *ConfigShowSuite) TestRedactsRenewToken() {
	in := "renew:\n" +
		"  endpoint: https://cp.example/v1/certs\n" +
		"  token: gmpat_supersecretbearer\n" +
		"  token_file: /etc/gmountie/token\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "gmpat_supersecretbearer", "inline token must be redacted")
	s.Contains(out, "token: REDACTED")
	// token_file is a path, not a secret — must render verbatim.
	s.Contains(out, "token_file: /etc/gmountie/token", "token_file path must not be redacted")
}

func (s *ConfigShowSuite) TestConfigShow_ProfileResolvesPath() {
	profileName = "work"
	defer func() { profileName = "" }()
	configFile = ""

	cfgHome := s.T().TempDir()
	s.T().Setenv("XDG_CONFIG_HOME", cfgHome)
	pdir := filepath.Join(cfgHome, "gmountie", "profiles")
	s.Require().NoError(os.MkdirAll(pdir, 0o755))
	profile := "server:\n  address: work.example.com\n  port: 9449\nauth:\n  type: basic\n  username: admin\n  password: supersecret\n"
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "work.yaml"), []byte(profile), 0o600))

	path, err := resolveConfigShowPath()
	s.Require().NoError(err)
	s.Equal(filepath.Join(pdir, "work.yaml"), path)
}
