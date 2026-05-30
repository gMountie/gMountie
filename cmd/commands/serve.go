//go:build linux

package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/pkg/common/passhash"
	"go.gmountie.dev/gmountie/pkg/server"
	serverConfig "go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/adrg/xdg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// firstRunConfigTemplate is the first-run server config. %s is the hashed
// admin password; %q is the volume data directory. Binds 0.0.0.0 so the
// server is reachable remotely out of the box (random password + auto TLS
// keep that safe); replace the hash via 'gmountie genpass' to rotate.
const firstRunConfigTemplate = `server:
  # 0.0.0.0 accepts remote connections. Set to 127.0.0.1 to restrict to localhost.
  address: 0.0.0.0
  port: 9449
  metrics: true

auth:
  type: basic
  users:
    - username: admin
      # Replace with the output of: gmountie genpass
      password_hash: %s

volumes:
  # Add or edit volumes here. Each exposes a server directory under a name.
  - name: shared
    path: %q
`

// buildFirstRunConfig generates the first-run server config. It returns the
// generated plaintext password (to print once), the YAML to write, and any
// error. The volume data dir is created (0700) by the caller before serving.
// Argon2id hashing (~300ms, 64 MiB) happens here — only when serve actually
// needs a first-run config — not at package init time.
func buildFirstRunConfig(dataDir string) (plaintext, yaml string, err error) {
	plaintext, err = passhash.GeneratePassphrase()
	if err != nil {
		return "", "", errors.Wrap(err, "generate admin password")
	}
	hash, err := passhash.Hash(plaintext)
	if err != nil {
		return "", "", errors.Wrap(err, "hash admin password")
	}
	return plaintext, fmt.Sprintf(firstRunConfigTemplate, hash, dataDir), nil
}

// defaultVolumeDir is the auto-created data directory for the first-run
// "shared" volume: $XDG_DATA_HOME/gmountie/shared.
func defaultVolumeDir() string {
	// Reload so $XDG_DATA_HOME / $HOME changes since package init are picked up,
	// mirroring GetDefaultConfigDir; otherwise xdg.DataHome holds its init value.
	xdg.Reload()
	return filepath.Join(xdg.DataHome, "gmountie", "shared")
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

			// Config doesn't exist — generate a usable first-run config.
			// buildFirstRunConfig runs argon2id hashing here (not at package
			// init) so other subcommands (version, fingerprint, genpass, …)
			// don't pay the ~300 ms / 64 MiB cost.
			log.Log.Info("no config file found, creating default configuration",
				zap.String("path", configFile))

			if err := config.EnsureConfigDir(); err != nil {
				return err
			}

			dataDir := defaultVolumeDir()
			if err := os.MkdirAll(dataDir, 0o700); err != nil {
				return errors.Wrapf(err, "create default volume dir %s", dataDir)
			}

			plaintext, generated, err := buildFirstRunConfig(dataDir)
			if err != nil {
				return err
			}
			if err := config.WriteDefaultConfig(config.DefaultServerConfigFileName, generated); err != nil {
				return err
			}

			// Print the generated password once — it is never stored in plaintext.
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
				"\n  ┌─ gMountie first run ───────────────────────────────\n"+
					"  │ A server config was created at:\n  │   %s\n"+
					"  │ Exposing volume \"shared\" at: %s\n"+
					"  │\n"+
					"  │ Login:    admin\n"+
					"  │ Password: %s\n"+
					"  │ (shown only now — save it; rotate with `gmountie genpass`)\n"+
					"  └────────────────────────────────────────────────────\n\n",
				configFile, dataDir, plaintext)

			cfgString = generated
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
