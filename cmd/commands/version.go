package commands

import (
	"gmountie/pkg"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Long:  `Print the version number of gMountie server`,
	Run: func(cmd *cobra.Command, args []string) {
		buildInfo := pkg.GetBuildInfo()
		cmd.Printf("Version: %s\n", buildInfo.Version)
		cmd.Printf("Commit: %s\n", buildInfo.Commit)
		cmd.Printf("Build Date: %s\n", buildInfo.Date)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
