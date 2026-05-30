//go:build linux

package commands

import (
	"context"
	"fmt"
	"io"
	"net"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var lsCmd = &cobra.Command{
	Use:   "ls [user@host[:port]]",
	Short: "List the volumes a gMountie server exposes",
	Long: "Connect to a server and list its available volumes.\n\n" +
		"  gmountie ls admin@host:9449\n" +
		"  gmountie ls -c client.yaml",
	Args: cobra.MaximumNArgs(1),
	RunE: runLs,
}

func init() {
	lsCmd.PersistentFlags().StringVarP(&authType, "auth-type", "t", "basic", "authentication type (basic)")
	lsCmd.PersistentFlags().StringVarP(&username, "username", "u", "", "username for basic auth")
	lsCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "password for basic auth (prefer prompt/$GMOUNTIE_AUTH_PASSWORD)")
	rootCmd.AddCommand(lsCmd)
}

func runLs(cmd *cobra.Command, args []string) error {
	v := viper.New()
	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read config file %s: %w", configFile, err)
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
	if err := resolveAuth(cmd, v); err != nil {
		return err
	}

	cfg, err := config.ParseConfig(v)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	addr := net.JoinHostPort(cfg.Server.Address, fmt.Sprintf("%d", cfg.Server.Port))

	c, err := grpc.NewClientFromConfig(cfg)
	if err != nil {
		return remediate(err, addr, "")
	}
	defer func() { _ = c.Close() }()

	reply, err := c.Volume().List(context.Background(), &proto.VolumeListRequest{})
	if err != nil {
		return remediate(err, addr, "")
	}
	renderVolumes(cmd.OutOrStdout(), reply.GetVolumes())
	return nil
}

// renderVolumes prints one volume name per line, or a friendly note if empty.
func renderVolumes(out io.Writer, vols []*proto.Volume) {
	if len(vols) == 0 {
		_, _ = fmt.Fprintln(out, "no volumes available")
		return
	}
	for _, vol := range vols {
		_, _ = fmt.Fprintln(out, vol.GetName())
	}
}
