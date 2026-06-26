package commands

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"go.gmountie.dev/gmountie/pkg/client/config"
)

// setOverrides holds the repeated `--set key=value` values for whichever client
// command runs. Shared package-global like profileName/credentialsFile.
var setOverrides []string

// addSetFlag registers the generic `--set key=value` config-override flag. The
// valid keys are derived by reflection from the config structs (see
// pkg/client/config/override.go), so any config value is reachable from the CLI
// without a bespoke flag per knob.
func addSetFlag(cmd *cobra.Command) {
	cmd.Flags().StringArrayVar(&setOverrides, "set", nil,
		"override a config value (repeatable): --set key=value, e.g. --set cache.enabled=false --set wal.enabled=true")
}

// applySetOverrides applies any --set overrides onto v. Call after the config
// file / credentials / spec are loaded and before ParseConfig so the overrides
// take the highest precedence (they beat the file, env, and other flags).
func applySetOverrides(v *viper.Viper) error {
	return config.ApplyOverrides(v, setOverrides)
}
