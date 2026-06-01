package commands

import (
	"fmt"
	"os"
	"strings"

	commonconfig "go.gmountie.dev/gmountie/pkg/common/config"

	"github.com/spf13/cobra"
)

// profileName is set by the --profile flag on whichever client command runs.
var profileName string

// addProfileFlag registers --profile (and its completion) on a command.
func addProfileFlag(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&profileName, "profile", "P", "",
		"named profile under ~/.config/gmountie/profiles/ (mutually exclusive with --config)")
	_ = cmd.RegisterFlagCompletionFunc("profile", profileNameCompletion)
}

// resolveProfilePath returns the config file path selected by --profile, or ""
// when --profile is unset (the caller then falls back to --config/defaults). It
// errors on --profile + --config together, an invalid name, or a missing profile.
func resolveProfilePath() (string, error) {
	if profileName == "" {
		return "", nil
	}
	if configFile != "" {
		return "", fmt.Errorf("use one of --profile or --config, not both")
	}
	if err := commonconfig.ValidateProfileName(profileName); err != nil {
		return "", err
	}
	path := commonconfig.GetProfilePath(profileName)
	if _, err := os.Stat(path); err != nil {
		names, _ := commonconfig.ListProfileNames()
		avail := "none"
		if len(names) > 0 {
			avail = strings.Join(names, ", ")
		}
		return "", fmt.Errorf("profile %q not found in %s (available: %s)",
			profileName, commonconfig.GetProfilesDir(), avail)
	}
	return path, nil
}

// profileNameCompletion completes --profile values from the profiles dir.
func profileNameCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	names, err := commonconfig.ListProfileNames()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return names, cobra.ShellCompDirectiveNoFileComp
}
