package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const overrideMinimalConf = `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
`

// TestOverridableKeys_KnownKeysPresent asserts the struct-derived key set
// contains real keys exercising every reflection branch: flat fields, untagged
// fields (server.address/port use the lowercased field name), nested structs
// (server.tls.*, rpc.keepalive.*), the inline-PEM fields, and the gaps the old
// hand-maintained mirror lists missed (cache.stat_fs_ttl/xattr_ttl,
// fuse.provider/fuset_backend). Per the UnmarshalExact backstop a reflection
// gap fails toward false-rejection, so these positive checks are sufficient.
func TestOverridableKeys_KnownKeysPresent(t *testing.T) {
	keys := overridableKeys()
	for _, k := range []string{
		"cache.enabled", "cache.path", "cache.attr_ttl",
		"cache.statfs_ttl", "cache.xattr_ttl", "cache.subscribe_enabled", // gaps old hand-list missed
		"wal.enabled",
		"fuse.attr_timeout", "fuse.provider", "fuse.fuset_backend", // fuse gaps
		"server.address", "server.port", "server.endpoint", // untagged fields
		"server.tls.verify", "server.tls.ca_pem", // nested + inline PEM
		"rpc.connections", "rpc.keepalive.time", "rpc.keepalive.permit_without_stream", // nested
		"renew.endpoint", "log.level", "log.format",
	} {
		assert.Truef(t, keys[k], "expected overridable key %q to be present", k)
	}
	// auth and mount are excluded (dedicated flags).
	assert.False(t, keys["auth.username"], "auth.* must not be overridable")
	assert.False(t, keys["mount.volume"], "mount.* must not be overridable")
}

func TestApplyOverrides_SetsValues(t *testing.T) {
	v := viper.New()
	require.NoError(t, ApplyOverrides(v, []string{
		"cache.enabled=false",
		"fuse.attr_timeout=9s",
		"rpc.connections=4",
	}))
	assert.Equal(t, "false", v.GetString("cache.enabled"))
	assert.Equal(t, "9s", v.GetString("fuse.attr_timeout"))
	assert.Equal(t, "4", v.GetString("rpc.connections"))
}

func TestApplyOverrides_CaseInsensitiveKey(t *testing.T) {
	v := viper.New()
	require.NoError(t, ApplyOverrides(v, []string{"Cache.Enabled=true"}))
	assert.True(t, v.IsSet("cache.enabled"), "key must normalize to lowercase")
}

func TestApplyOverrides_NilAndEmpty(t *testing.T) {
	v := viper.New()
	require.NoError(t, ApplyOverrides(v, nil))
	require.NoError(t, ApplyOverrides(v, []string{}))
}

func TestApplyOverrides_Errors(t *testing.T) {
	v := viper.New()
	require.Error(t, ApplyOverrides(v, []string{"noequals"}), "missing = must error")
	require.Error(t, ApplyOverrides(v, []string{"=value"}), "empty key must error")

	err := ApplyOverrides(v, []string{"cache.bogus=1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cache", "should name the section whose key was wrong")

	err = ApplyOverrides(v, []string{"nosuchsection.key=1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "section")

	// auth/mount point at their dedicated flags rather than a bare unknown-key.
	err = ApplyOverrides(v, []string{"auth.username=bob"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--username")
	err = ApplyOverrides(v, []string{"mount.volume=data"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--volume")
}

// TestApplyOverrides_ThroughParseConfig_BeatsEnv proves end-to-end: a --set wins
// over an env var (viper Set is the highest precedence), the string value is
// coerced to the target type, and rpc.* — newly routed through mirrorEnvToSub —
// is now overridable.
func TestApplyOverrides_ThroughParseConfig_BeatsEnv(t *testing.T) {
	t.Setenv("GMOUNTIE_CACHE_ENABLED", "false")

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(overrideMinimalConf)))
	require.NoError(t, ApplyOverrides(v, []string{
		"cache.enabled=true", // beats GMOUNTIE_CACHE_ENABLED=false
		"rpc.connections=4",
		"fuse.attr_timeout=7s",
	}))

	cfg, err := ParseConfig(v)
	require.NoError(t, err)
	assert.True(t, cfg.Cache.Enabled, "--set cache.enabled=true must beat env =false")
	assert.Equal(t, 4, cfg.Rpc.Connections, "rpc is now --set-overridable")
	assert.Equal(t, 7*time.Second, cfg.FUSE.AttrTimeout, "string value coerced to duration")
}
