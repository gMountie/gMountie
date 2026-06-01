//go:build linux || darwin

package commands

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/mount"
	"go.gmountie.dev/gmountie/pkg/utils/log"

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

// resolveAuth fills viper's auth.* keys for a client command (mount/ls),
// resolving the password without leaving it on the command line.
//
// auth.type falls back to the flag default ("basic") when empty, so a config
// file that omits it still authenticates. For basic auth it writes the COMPLETE
// tuple (type, username, password) as a single override: viper's Sub("auth")
// (used by ParseConfig) does not deep-merge a partial override over the
// config-file map, so setting only auth.password would otherwise drop a config
// file's auth.username. Non-basic auth leaves the config sub-tree untouched.
//
// Password precedence: --password flag > config file / spec > $GMOUNTIE_AUTH_PASSWORD > prompt.
func resolveAuth(cmd *cobra.Command, v *viper.Viper) error {
	authTypeVal := v.GetString("auth.type")
	if authTypeVal == "" || cmd.Flags().Changed("auth-type") {
		authTypeVal = authType
	}
	if authTypeVal != "basic" {
		return nil
	}

	user := v.GetString("auth.username")
	if cmd.Flags().Changed("username") {
		user = username
	}
	if user == "" {
		return fmt.Errorf("username is required for basic auth (use user@host or -u)")
	}

	pw := v.GetString("auth.password")
	if cmd.Flags().Changed("password") {
		pw = password
	} else if pw == "" {
		resolved, err := resolvePassword("", cmd.InOrStdin(), cmd.ErrOrStderr())
		if err != nil {
			return err
		}
		pw = resolved
	}

	v.Set("auth.type", "basic")
	v.Set("auth.username", user)
	v.Set("auth.password", pw)
	return nil
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

		if volumeName == "" {
			return fmt.Errorf("volume name is required (use the shorthand host/volume or -n)")
		}

		if err := resolveAuth(cmd, v); err != nil {
			return err
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

		// Create client (follows a VolumeService.Resolve referral if the
		// server points this volume at a different data-plane location).
		c, err := grpc.NewClientForVolume(cfg, volumeName)
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

		// Record this mount so `gmountie status` can list it and `gmountie
		// unmount` can stop it; clear the record on exit. Best-effort — a
		// state-file failure must not bring down a working mount.
		if err := writeMountState(mountState{
			Mountpoint: mountpoint,
			Server:     addr,
			Volume:     volumeName,
			PID:        os.Getpid(),
		}); err != nil {
			log.Log.Warn("could not record mount state", zap.Error(err))
		}
		defer func() { _ = removeMountState(mountpoint) }()

		// Release a waiting --daemon parent now that the mount is up (no-op
		// unless this process is the detached child).
		signalDaemonReady()

		log.Log.Sugar().Info("Press Ctrl+C to unmount")

		// Wait until either the user signals us (Ctrl-C / SIGTERM) or the FUSE
		// filesystem is detached out-of-band (e.g. someone runs `fusermount -u`
		// directly). Without the latter, an external unmount would leave this
		// process blocked on the signal wait forever — see mounter.Wait.
		served := make(chan struct{})
		go func() {
			mounter.Wait(volumeName)
			close(served)
		}()

		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		if waitForUnmount(sig, served) {
			log.Log.Sugar().Infof("Volume %s was unmounted; exiting", volumeName)
		}

		return nil
	},
}

// waitForUnmount blocks until either an OS signal arrives or the served channel
// is closed (the FUSE server exited, e.g. an external unmount). It returns true
// when the wait ended because the filesystem went away rather than a signal.
func waitForUnmount(sig <-chan os.Signal, served <-chan struct{}) (external bool) {
	select {
	case <-sig:
		return false
	case <-served:
		return true
	}
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
