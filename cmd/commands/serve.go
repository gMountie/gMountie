//go:build linux

package commands

import (
	"context"
	"fmt"
	"gmountie/pkg/common/config"
	"gmountie/pkg/common/passhash"
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

// defaultConfigTemplate is the first-run server config. The %s placeholder
// is replaced at startup with a freshly-hashed "admin" password so the
// on-disk YAML always contains a valid $argon2id$ PHC string. Users should
// replace the hash with the output of 'gmountie genpass' before deploying.
const defaultConfigTemplate = `server:
  address: 127.0.0.1
  port: 9449
  metrics: true

auth:
  type: basic
  users:
    - username: admin
      # Replace with the output of: gmountie genpass
      password_hash: %s
`

// buildDefaultConfig generates the first-run server configuration with a
// freshly-hashed "admin" password. Argon2id hashing is expensive (~300ms,
// 64 MiB), so this is called only when the serve command actually needs to
// write a first-run config — not at package init time.
func buildDefaultConfig() (string, error) {
	h, err := passhash.Hash("admin")
	if err != nil {
		return "", fmt.Errorf("serve: generate default admin hash: %w", err)
	}
	return fmt.Sprintf(defaultConfigTemplate, h), nil
}

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

			// Config doesn't exist, create default one.
			// buildDefaultConfig runs argon2id hashing here (not at package
			// init) so other subcommands (version, fingerprint, genpass, …)
			// don't pay the ~300 ms / 64 MiB cost.
			log.Log.Info("no config file found, creating default configuration",
				zap.String("path", configFile))

			dc, err := buildDefaultConfig()
			if err != nil {
				return err
			}

			if err := config.EnsureConfigDir(); err != nil {
				return err
			}

			if err := config.WriteDefaultConfig(config.DefaultServerConfigFileName, dc); err != nil {
				return err
			}

			cfgString = dc
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
