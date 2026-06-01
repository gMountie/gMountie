package commands

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type EffectiveConfigSuite struct{ suite.Suite }

func TestEffectiveConfigSuite(t *testing.T) { suite.Run(t, new(EffectiveConfigSuite)) }

type SampleBase struct {
	Type string `mapstructure:"type"`
}

type sampleTLS struct {
	KeyPEM  string `mapstructure:"key_pem"`
	CertPEM string `mapstructure:"cert_pem"`
}

type sampleCfg struct {
	SampleBase    `mapstructure:",squash"`
	MaxWriteBytes int           `mapstructure:"max_write_bytes"`
	TTL           time.Duration `mapstructure:"ttl"`
	TLS           *sampleTLS    `mapstructure:"tls"`
	Items         []string      `mapstructure:"items"`
	Optional      *sampleTLS    `mapstructure:"optional"`
	Untagged      string
	Skip          string `mapstructure:"-"`
	unexported    string //nolint:unused
}

func (s *EffectiveConfigSuite) sample() sampleCfg {
	return sampleCfg{
		SampleBase:    SampleBase{Type: "basic"},
		MaxWriteBytes: 100,
		TTL:           5 * time.Second,
		TLS:           &sampleTLS{KeyPEM: "SECRETKEY", CertPEM: "PUBLICCERT"},
		Items:         []string{"a", "b"},
		Untagged:      "hi",
		Skip:          "dropme",
		unexported:    "x",
	}
}

// TestMarshalEffective_UsesMapstructureKeys proves the renderer emits the
// snake_case mapstructure key names (so the output is a valid config file),
// formats time.Duration as a human string, flattens squashed/embedded structs,
// recurses pointers/slices, and drops "-" and unexported fields.
func (s *EffectiveConfigSuite) TestMarshalEffective_UsesMapstructureKeys() {
	out, err := marshalEffective(s.sample())
	s.Require().NoError(err)

	s.Contains(out, "max_write_bytes: 100") // snake_case from the tag
	s.Contains(out, "ttl: 5s")              // Duration rendered as a string
	s.Contains(out, "type: basic")          // squashed embedded field, flattened
	s.Contains(out, "key_pem: SECRETKEY")   // nested via pointer
	s.Contains(out, "untagged: hi")         // untagged field falls back to lower-case name

	s.NotContains(out, "Skip")   // mapstructure:"-" dropped
	s.NotContains(out, "dropme") // ...value gone too
	s.NotContains(out, "unexported")
}

// TestEffective_RedactsSecretsButKeepsPublic proves the effective render runs
// through the same secret redactor: a private key_pem is redacted while the
// public cert_pem survives.
func (s *EffectiveConfigSuite) TestEffective_RedactsSecretsButKeepsPublic() {
	out, err := marshalEffective(s.sample())
	s.Require().NoError(err)
	redacted := redactConfigYAML(out)

	s.NotContains(redacted, "SECRETKEY")
	s.Contains(redacted, "REDACTED")
	s.Contains(redacted, "PUBLICCERT") // public material stays visible
}

// TestMarshalEffective_NilPointerIsNull proves an unset optional pointer
// renders as an explicit null rather than panicking.
func (s *EffectiveConfigSuite) TestMarshalEffective_NilPointerIsNull() {
	m := structToDisplayMap(reflect.ValueOf(s.sample()))
	asMap, ok := m.(map[string]any)
	s.Require().True(ok)
	s.Nil(asMap["optional"])
}

// TestRenderEffectiveConfig_MergesDefaultsAndRedacts exercises the real command
// path: a sparse client config is resolved through ParseConfig (so defaults are
// merged), rendered in snake_case, and secrets redacted while public fields and
// the operator's explicit values survive.
func (s *EffectiveConfigSuite) TestRenderEffectiveConfig_MergesDefaultsAndRedacts() {
	dir := s.T().TempDir()
	path := filepath.Join(dir, "client.yaml")
	cfg := "server:\n" +
		"  address: example.com\n" +
		"  port: 9449\n" +
		"  tls:\n" +
		"    key_pem: |\n" +
		"      -----BEGIN PRIVATE KEY-----\n" +
		"      LEAKEDKEY\n" +
		"      -----END PRIVATE KEY-----\n" +
		"auth:\n" +
		"  type: basic\n" +
		"  username: admin\n" +
		"  password: supersecret\n"
	s.Require().NoError(os.WriteFile(path, []byte(cfg), 0o600))

	out, err := renderEffectiveConfig(path, "client")
	s.Require().NoError(err)

	// Defaults the file omitted are now present (proves the merge).
	s.Contains(out, "max_write_bytes:")
	// A default Duration renders as a human string, not nanoseconds.
	s.Contains(out, "5m0s")
	// Secrets are redacted; the file's explicit non-secret values survive.
	s.NotContains(out, "supersecret")
	s.NotContains(out, "LEAKEDKEY")
	s.Contains(out, "REDACTED")
	s.Contains(out, "address: example.com")
}
