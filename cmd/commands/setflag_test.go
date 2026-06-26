package commands

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetFlagRegistered locks the --set flag wiring onto the client commands
// that load config: a regression that drops addSetFlag from either command
// would silently lose the override capability.
func TestSetFlagRegistered(t *testing.T) {
	require.NotNil(t, newMountCmd().Flags().Lookup("set"), "mount must register --set")
	require.NotNil(t, newLsCmd().Flags().Lookup("set"), "ls must register --set")
}

// TestApplySetOverrides_DelegatesAndValidates confirms the command-layer helper
// applies valid overrides onto the viper and surfaces validation errors.
func TestApplySetOverrides_DelegatesAndValidates(t *testing.T) {
	orig := setOverrides
	t.Cleanup(func() { setOverrides = orig })

	v := viper.New()
	setOverrides = []string{"cache.enabled=false", "wal.enabled=true"}
	require.NoError(t, applySetOverrides(v))
	assert.Equal(t, "false", v.GetString("cache.enabled"))
	assert.Equal(t, "true", v.GetString("wal.enabled"))

	setOverrides = []string{"cache.bogus=1"}
	require.Error(t, applySetOverrides(v), "unknown key must error")
}
