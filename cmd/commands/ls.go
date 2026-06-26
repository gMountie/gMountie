//go:build linux || darwin

package commands

import (
	"fmt"
	"io"
	"net"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// newLsCmd constructs the ls command with its own flag state (closure
// pattern), so ls and mount no longer share mutable package-level globals.
func newLsCmd() *cobra.Command {
	f := &authFlags{}
	tf := &rpcTimeoutFlags{}
	cmd := &cobra.Command{
		Use:   "ls [user@host[:port]]",
		Short: "List the volumes a gMountie server exposes",
		Long: "Connect to a server and list its available volumes.\n\n" +
			"  gmountie ls admin@host:9449\n" +
			"  gmountie ls -c client.yaml",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLs(cmd, args, f, tf)
		},
	}
	addProfileFlag(cmd)
	addSetFlag(cmd)
	addAuthFlags(cmd, f)
	addCredentialsFlag(cmd)
	addRpcTimeoutFlags(cmd, tf)
	return cmd
}

func init() {
	rootCmd.AddCommand(newLsCmd())
}

func runLs(cmd *cobra.Command, args []string, f *authFlags, tf *rpcTimeoutFlags) error {
	profilePath, err := resolveProfilePath()
	if err != nil {
		return err
	}
	cfgPath := configFile
	if profilePath != "" {
		cfgPath = profilePath
	}

	v := viper.New()
	if cfgPath != "" {
		v.SetConfigFile(cfgPath)
		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read config file %s: %w", cfgPath, err)
		}
	}
	if len(args) == 1 {
		// Reuse the spec parser for the host portion by appending a sentinel
		// volume, which ls does not need.
		spec, err := parseMountSpec(args[0] + "/_")
		if err != nil {
			return err
		}
		applyMountSpec(v, spec)
	}

	// Apply a single-blob credential (--credentials / $GMOUNTIE_CREDENTIALS)
	// over the config file but under explicit flags, exactly as mount does. It
	// supplies the resolver endpoint + mTLS material + auth.type=mtls, so
	// `GMOUNTIE_CREDENTIALS=... gmountie ls` lists with no host arg and no -u
	// (mtls auth means resolveAuth skips the basic-auth username requirement).
	if _, err := applyCredentialToViper(v); err != nil {
		return err
	}
	if err := resolveAuth(cmd, v, f); err != nil {
		return err
	}
	applyRpcTimeoutFlags(cmd, v, tf)

	// Generic --set overrides win over the config file, env, and flags above.
	if err := applySetOverrides(v); err != nil {
		return err
	}

	cfg, err := config.ParseConfig(v)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	if err := applyClientLogConfig(cfg); err != nil {
		return err
	}
	addr := net.JoinHostPort(cfg.Server.Address, fmt.Sprintf("%d", cfg.Server.Port))

	// List over a PRE-SESSION VolumeService.List call (no session handshake) so
	// ls works against the cloud resolver, which implements only VolumeService.
	// The mTLS cert / basic-auth creds authenticate the per-RPC List call.
	names, err := grpc.ListVolumes(cfg)
	if err != nil {
		return remediate(err, addr, "")
	}
	renderVolumes(cmd.OutOrStdout(), names)
	return nil
}

// renderVolumes prints one volume name per line, or a friendly note if empty.
func renderVolumes(out io.Writer, names []string) {
	if len(names) == 0 {
		_, _ = fmt.Fprintln(out, "no volumes available")
		return
	}
	for _, name := range names {
		_, _ = fmt.Fprintln(out, name)
	}
}
