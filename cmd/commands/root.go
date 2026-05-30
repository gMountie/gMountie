package commands

import (
	"fmt"
	"os"

	"go.gmountie.dev/gmountie/pkg/common"

	"github.com/spf13/cobra"
)

var (
	configFile string
	verbose    bool
)

var rootCmd = &cobra.Command{
	Use:   common.AppName,
	Short: "gMountie - A network filesystem server",
	Long: `gMountie is a high-performance network filesystem server 
that allows sharing directories over the network using FUSE.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "",
		"path to a server.yaml or client.yaml; explicit flags override file values, which override $GMOUNTIE_* env vars")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false,
		"enable verbose (debug-level) logging")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
