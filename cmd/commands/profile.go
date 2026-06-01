package commands

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	commonconfig "go.gmountie.dev/gmountie/pkg/common/config"
)

var profileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Manage gMountie client config profiles",
}

var profileListCmd = &cobra.Command{
	Use:   "list",
	Short: "List the client config profiles",
	RunE:  runProfileList,
}

func init() {
	profileCmd.AddCommand(profileListCmd)
	rootCmd.AddCommand(profileCmd)
}

func runProfileList(cmd *cobra.Command, _ []string) error {
	names, err := commonconfig.ListProfileNames()
	if err != nil {
		return err
	}
	out := cmd.OutOrStdout()
	if len(names) == 0 {
		_, _ = fmt.Fprintf(out, "No profiles in %s\n", commonconfig.GetProfilesDir())
		return nil
	}
	for _, name := range names {
		_, _ = fmt.Fprintf(out, "%s\t%s\n", name, profileSummary(name))
	}
	return nil
}

// profileSummary returns a best-effort "address:port/volume" line for a profile.
// Parsing failures yield an empty summary rather than an error — list must not
// fail because one profile is malformed.
func profileSummary(name string) string {
	v := viper.New()
	v.SetConfigFile(commonconfig.GetProfilePath(name))
	if err := v.ReadInConfig(); err != nil {
		return ""
	}
	addr := v.GetString("server.address")
	port := v.GetInt("server.port")
	vol := v.GetString("mount.volume")
	if addr == "" {
		return ""
	}
	summary := addr
	if port != 0 {
		summary = fmt.Sprintf("%s:%d", addr, port)
	}
	if vol != "" {
		summary += "/" + vol
	}
	return summary
}
