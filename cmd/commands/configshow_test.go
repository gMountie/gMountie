package commands

import (
	"testing"

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
