//go:build linux

package commands

import (
	"fmt"
	"gmountie/pkg/client/config"
	"gmountie/pkg/client/grpc"
	"gmountie/pkg/client/mount"
	"gmountie/pkg/utils/log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// startPprofIfEnabled starts a /debug/pprof HTTP listener if the env var
// GMOUNTIE_PPROF_ADDR is set (e.g. "127.0.0.1:6060"). Diagnostic only —
// kept env-gated rather than wired through Config because it's a
// debugger hook, not a runtime feature.
func startPprofIfEnabled() {
	addr := os.Getenv("GMOUNTIE_PPROF_ADDR")
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	go func() {
		log.Log.Sugar().Infof("pprof listening on %s/debug/pprof/", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Log.Error("pprof server stopped", zap.Error(err))
		}
	}()
}

var (
	serverAddr string
	volumeName string
	authType   string
	username   string
	password   string
	rawIDs     bool
	daemonFlag bool
	cfg        *config.Config
)

// applyMountSpec maps a parsed shorthand spec onto the viper instance and
// returns the volume name it carried. Explicit flags (checked by the caller)
// still take precedence over these values.
func applyMountSpec(v *viper.Viper, spec mountSpec) string {
	v.Set("server.address", spec.Host)
	v.Set("server.port", fmt.Sprintf("%d", spec.Port))
	if spec.Username != "" {
		v.Set("auth.username", spec.Username)
	}
	return spec.Volume
}

var mountCmd = &cobra.Command{
	Use:   "mount [user@host[:port]/volume] mountpoint",
	Short: "Mount a gMountie volume",
	Long: "Mount a gMountie volume at the given mountpoint.\n\n" +
		"Shorthand:  gmountie mount admin@host:9449/shared /mnt/shared\n" +
		"Or flags:   gmountie mount /mnt/shared -s host:9449 -n shared -u admin\n\n" +
		"The password is taken from --password, then the config file, then\n" +
		"$GMOUNTIE_AUTH_PASSWORD, then an interactive prompt. Use --daemon to\n" +
		"mount in the background.",
	Args: cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Build a viper instance, optionally seeded from a config file, then
		// layer the shorthand spec and explicit CLI flags on top.
		//
		// Precedence (highest first): explicit flag > shorthand spec > config
		// file > flag default. The shorthand is typed explicitly on the command
		// line, so it wins over a config file; a config file's values must not
		// be silently shadowed by flag defaults the user never set.
		v := viper.New()
		hasConfig := configFile != ""
		if hasConfig {
			v.SetConfigFile(configFile)
			if err := v.ReadInConfig(); err != nil {
				return fmt.Errorf("failed to read config file %s: %w", configFile, err)
			}
		}

		// Positional forms:
		//   2 args: "<spec> <mountpoint>" — spec seeds server/user/volume
		//   1 arg : "<mountpoint>"        — flags/config supply the rest
		var mountpoint string
		usedSpec := len(args) == 2
		if usedSpec {
			spec, err := parseMountSpec(args[0])
			if err != nil {
				return err
			}
			vol := applyMountSpec(v, spec)
			if volumeName == "" {
				volumeName = vol
			}
			mountpoint = args[1]
		} else {
			mountpoint = args[0]
		}

		// -s "host:port" overrides the spec/file only when explicitly set (or
		// when there's neither a config file nor a shorthand spec to supply it).
		// net.SplitHostPort handles IPv6 bracket notation ([::1]:9449) correctly.
		if cmd.Flags().Changed("server") || (!hasConfig && !usedSpec) {
			host, port, err := net.SplitHostPort(serverAddr)
			if err != nil {
				return fmt.Errorf("invalid server address %q: %w", serverAddr, err)
			}
			v.Set("server.address", host)
			v.Set("server.port", port)
		}

		// Layer explicit flags; fall back to a flag default only when nothing
		// (config or spec) already supplied the value.
		setFromFlag := func(name, viperKey, value string) {
			if cmd.Flags().Changed(name) || (!hasConfig && v.GetString(viperKey) == "") {
				v.Set(viperKey, value)
			}
		}
		setFromFlag("auth-type", "auth.type", authType)
		setFromFlag("username", "auth.username", username)

		if volumeName == "" {
			return fmt.Errorf("volume name is required (use the shorthand host/volume or -n)")
		}

		// Resolve the basic-auth password without leaving it on the command
		// line: explicit --password wins, then the config file's value, then
		// $GMOUNTIE_AUTH_PASSWORD, then an interactive prompt.
		if v.GetString("auth.type") == "basic" {
			if v.GetString("auth.username") == "" {
				return fmt.Errorf("username is required for basic auth (use user@host or -u)")
			}
			if cmd.Flags().Changed("password") {
				v.Set("auth.password", password)
			} else if v.GetString("auth.password") == "" {
				pw, err := resolvePassword("", cmd.InOrStdin(), cmd.ErrOrStderr())
				if err != nil {
					return err
				}
				v.Set("auth.password", pw)
			}
		}

		var err error
		cfg, err = config.ParseConfig(v)
		if err != nil {
			return fmt.Errorf("failed to parse config: %w", err)
		}

		// raw IDs are enabled by either the --raw-ids flag or `mount.raw_ids`
		// in the config file (it's opt-in; either source turns it on).
		if sm, ok := cfg.Mount.(*config.SingleMountConfig); ok && sm.RawIDs {
			rawIDs = true
		}

		// Verify that the mountpoint directory exists
		if _, err := os.Stat(mountpoint); os.IsNotExist(err) {
			return fmt.Errorf("mountpoint %s does not exist", mountpoint)
		}

		addr := net.JoinHostPort(cfg.Server.Address, fmt.Sprintf("%d", cfg.Server.Port))

		// Background mode: hand off to a detached child, passing the resolved
		// password through the environment (the child has no TTY to prompt).
		if daemonFlag {
			if pw := v.GetString("auth.password"); pw != "" {
				_ = os.Setenv(passwordEnvVar, pw)
			}
			return daemonize(execDaemonizer{}, os.Args)
		}

		startPprofIfEnabled()

		// Create client
		c, err := grpc.NewClientFromConfig(cfg)
		if err != nil {
			return remediate(err, addr, volumeName)
		}

		defer func(c grpc.Client) {
			err := c.Close()
			if err != nil {
				log.Log.Error("failed to close the client", zap.Error(err))
			}
		}(c)

		// Create mounter
		mounter := mount.NewSingleVolumeMounter(c, cfg.FUSE, *cfg.Cache, rawIDs)
		defer func(mounter mount.SingleVolumeMounter) {
			err := mounter.Close()
			if err != nil {
				log.Log.Error("failed to close the mounter", zap.Error(err))
			}
		}(mounter)

		// Mount volume
		if err := mounter.Mount(volumeName, mountpoint); err != nil {
			return remediate(err, addr, volumeName)
		}

		log.Log.Sugar().Infof("Mounted volume %s at %s", volumeName, mountpoint)

		// Release a waiting --daemon parent now that the mount is up (no-op
		// unless this process is the detached child).
		signalDaemonReady()

		log.Log.Sugar().Info("Press Ctrl+C to unmount")

		// Wait for interrupt signal
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch

		return nil
	},
}

func init() {
	mountCmd.PersistentFlags().StringVarP(&serverAddr, "server", "s", "127.0.0.1:9449", "server address")
	mountCmd.PersistentFlags().StringVarP(&volumeName, "volume", "n", "", "volume name")
	mountCmd.PersistentFlags().StringVarP(&authType, "auth-type", "t", "basic", "authentication type (basic)")
	mountCmd.PersistentFlags().StringVarP(&username, "username", "u", "", "username for basic auth")
	mountCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "password for basic auth (visible in ps/history; prefer the prompt or $GMOUNTIE_AUTH_PASSWORD)")
	mountCmd.PersistentFlags().BoolVar(&rawIDs, "raw-ids", false, "expose server-side uids/gids unchanged (for backups/admin tooling)")
	mountCmd.PersistentFlags().BoolVar(&daemonFlag, "daemon", false, "mount in the background (detach after the mount is ready)")
	rootCmd.AddCommand(mountCmd)
}
