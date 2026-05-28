//go:build linux

package commands

import (
	"context"
	"gmountie/pkg/common/config"
	"gmountie/pkg/server"
	serverConfig "gmountie/pkg/server/config"
	"gmountie/pkg/utils/log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	DefaultConfig = `server:
  address: 127.0.0.1
  port: 9449
  metrics: true

auth:
  type: basic
  users:
    - username: admin
      password: admin
`
)

// For testing purposes
var serverStart = server.Start

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the gMountie server",
	Long:  `Start the gMountie server using the specified configuration file.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			cfgString string
			err       error
		)

		if configFile == "" {
			configFile = config.GetDefaultConfigPath(config.DefaultServerConfigFileName)
		}

		// Try to read the config file
		data, err := os.ReadFile(configFile)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}

			// Config doesn't exist, create default one
			log.Log.Info("no config file found, creating default configuration",
				zap.String("path", configFile))

			if err := config.EnsureConfigDir(); err != nil {
				return err
			}

			if err := config.WriteDefaultConfig(config.DefaultServerConfigFileName, DefaultConfig); err != nil {
				return err
			}

			cfgString = DefaultConfig
		} else {
			cfgString = string(data)
		}

		// Parse config
		cfg, err := serverConfig.LoadConfigFromString(cfgString)
		if err != nil {
			return errors.Wrap(err, "failed to parse config")
		}

		// Start server
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		return serverStart(ctx, cfg)
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}
