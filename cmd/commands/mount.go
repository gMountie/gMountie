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
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/mount"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/prometheus/client_golang/prometheus/promhttp"
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

// startClientMetricsIfEnabled starts a /metrics HTTP listener if the env var
// GMOUNTIE_METRICS_ADDR is set (e.g. "127.0.0.1:9100"). The client's gRPC
// collectors are registered on prometheus.DefaultRegisterer (factory.go), but
// `gmountie mount` otherwise exposes nothing to scrape. Env-gated like pprof —
// an observability hook, not a runtime feature, so no config knob.
func startClientMetricsIfEnabled() {
	addr := os.Getenv("GMOUNTIE_METRICS_ADDR")
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Log.Sugar().Infof("client metrics listening on %s/metrics", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Log.Error("client metrics server stopped", zap.Error(err))
		}
	}()
}

// authFlags is the basic-auth flag set used by both mount and ls. Each
// command binds its OWN instance (closure pattern), so the two commands no
// longer share mutable package-level state and tests need no hand-resetting.
type authFlags struct {
	authType string
	username string
	password string
}

// addAuthFlags registers -t/-u/-p on cmd, bound to f.
func addAuthFlags(cmd *cobra.Command, f *authFlags) {
	cmd.PersistentFlags().StringVarP(&f.authType, "auth-type", "t", "basic", "authentication type (basic)")
	cmd.PersistentFlags().StringVarP(&f.username, "username", "u", "", "username for basic auth")
	cmd.PersistentFlags().StringVarP(&f.password, "password", "p", "", "password for basic auth (visible in ps/history; prefer the prompt or $GMOUNTIE_AUTH_PASSWORD)")
}

// mountFlags holds `gmountie mount`'s flag state. A fresh instance is bound
// per command construction (newMountCmd).
type mountFlags struct {
	serverAddr string
	volumeName string
	auth       authFlags
	rpc        rpcTimeoutFlags
	rawIDs     bool
	daemon     bool
}

// applyVerbose forces debug-level logging onto cfg when the global --verbose
// flag is set, allocating cfg.Log if the config had no log block. Pure (no
// process side effects) so the --verbose→config wiring is unit-testable. An
// explicit log.level in the config is overridden: --verbose is a deliberate
// per-invocation override.
func applyVerbose(cfg *config.Config, verbose bool) {
	if cfg == nil || !verbose {
		return
	}
	if cfg.Log == nil {
		cfg.Log = &log.LogConfig{}
	}
	cfg.Log.Level = "debug"
}

// applyClientLogConfig applies the parsed config's log block to the package
// logger, honoring the global --verbose flag first. This used to happen as a
// constructor side effect inside the client factory (pkg/client/grpc); the CLI
// owns logger configuration now, so library consumers of pkg/client keep their
// own logger (see pkg/utils/log).
//
// --verbose must apply even when the config has no log block, so the nil-Log
// early return is gated on it: applyVerbose allocates cfg.Log when verbose is
// set, and only a still-nil Log (no verbose, no config block) short-circuits.
func applyClientLogConfig(cfg *config.Config) error {
	if cfg == nil {
		return nil
	}
	applyVerbose(cfg, verbose)
	if cfg.Log == nil {
		return nil
	}
	if err := log.Reconfigure(*cfg.Log, os.Stderr); err != nil {
		return fmt.Errorf("configure logger: %w", err)
	}
	return nil
}

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
func resolveAuth(cmd *cobra.Command, v *viper.Viper, f *authFlags) error {
	authTypeVal := v.GetString("auth.type")
	if authTypeVal == "" || cmd.Flags().Changed("auth-type") {
		authTypeVal = f.authType
	}
	if authTypeVal != "basic" {
		return nil
	}

	user := v.GetString("auth.username")
	if cmd.Flags().Changed("username") {
		user = f.username
	}
	if user == "" {
		return fmt.Errorf("username is required for basic auth (use user@host or -u)")
	}

	var pw string
	if cmd.Flags().Changed("password") {
		pw = f.password
	} else {
		configured, err := resolveConfiguredPassword(cmd.Context(), v)
		if err != nil {
			return err
		}
		pw = configured
		if pw == "" {
			resolved, err := resolvePassword("", cmd.InOrStdin(), cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			pw = resolved
		}
	}

	v.Set("auth.type", "basic")
	v.Set("auth.username", user)
	v.Set("auth.password", pw)
	return nil
}

// mountTarget bundles the fully-resolved inputs runMount needs: the parsed
// client config plus the volume, mountpoint and presentation strings derived
// from flag/spec/profile/credential layering in buildMountConfig.
type mountTarget struct {
	cfg        *config.Config
	volume     string
	mountpoint string
	addr       string
	// password is the resolved basic-auth password, carried for the --daemon
	// pipe hand-off (the detached child has no TTY to prompt; the secret is
	// passed over fd 4, never the environment — see daemon.go / CQ-L2).
	password string
	rawIDs   bool
}

// buildMountConfig layers profile/config file, shorthand spec, credential
// blob and explicit CLI flags into a parsed config and resolves the volume
// and mountpoint. Pure resolution + validation: no networking, no process
// side effects beyond logger configuration.
//
// Precedence (highest first): explicit flag > shorthand spec > config
// file > flag default. The shorthand is typed explicitly on the command
// line, so it wins over a config file; a config file's values must not
// be silently shadowed by flag defaults the user never set.
func buildMountConfig(cmd *cobra.Command, args []string, f *mountFlags) (*mountTarget, error) {
	profilePath, err := resolveProfilePath()
	if err != nil {
		return nil, err
	}
	cfgPath := configFile
	if profilePath != "" {
		cfgPath = profilePath
	}

	v := viper.New()
	hasConfig := cfgPath != ""
	if hasConfig {
		v.SetConfigFile(cfgPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file %s: %w", cfgPath, err)
		}
	}

	// Positional forms:
	//   2 args: "<spec> <mountpoint>" — spec seeds server/user/volume
	//   1 arg : "<mountpoint>"        — flags/config supply the rest
	//   0 args: profile supplies mountpoint via mount.path
	volume := f.volumeName
	var mountpoint string
	usedSpec := len(args) == 2
	switch len(args) {
	case 2:
		spec, err := parseMountSpec(args[0])
		if err != nil {
			return nil, err
		}
		vol := applyMountSpec(v, spec)
		if volume == "" {
			volume = vol
		}
		mountpoint = args[1]
	case 1:
		mountpoint = args[0]
	}

	// Decode a single-blob credential (file flag or $GMOUNTIE_CREDENTIALS)
	// and apply it over the config file but under explicit CLI flags. This
	// supplies the endpoint + mTLS material + auth.type=mtls so the client
	// can mount with cert-only auth and no config file.
	credApplied, err := applyCredentialToViper(v)
	if err != nil {
		return nil, err
	}

	// -s "host:port" overrides the spec/file only when explicitly set (or
	// when there's neither a config file, a shorthand spec, nor a credential
	// to supply it). Without the `!credApplied` guard the -s default
	// (127.0.0.1:9449) would clobber the credential's endpoint.
	// net.SplitHostPort handles IPv6 bracket notation ([::1]:9449) correctly.
	if cmd.Flags().Changed("server") || (!hasConfig && !usedSpec && !credApplied) {
		host, port, err := net.SplitHostPort(f.serverAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid server address %q: %w", f.serverAddr, err)
		}
		v.Set("server.address", host)
		v.Set("server.port", port)
	}

	if err := resolveAuth(cmd, v, &f.auth); err != nil {
		return nil, err
	}
	applyRpcTimeoutFlags(cmd, v, &f.rpc)

	cfg, err := config.ParseConfig(v)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	if err := applyClientLogConfig(cfg); err != nil {
		return nil, err
	}

	// Fall back to the profile/config mount block for anything the CLI omitted.
	rawIDs := f.rawIDs
	if sm, ok := cfg.Mount.(*config.SingleMountConfig); ok {
		if volume == "" {
			volume = sm.Volume
		}
		if mountpoint == "" {
			mountpoint = sm.Path
		}
		// raw IDs are enabled by either the --raw-ids flag or `mount.raw_ids`
		// in the config file (it's opt-in; either source turns it on).
		if sm.RawIDs {
			rawIDs = true
		}
	}
	// An empty volume name is allowed here: NewClientForVolume lists the
	// caller's volumes and auto-selects the sole one (erroring on zero or
	// many). An explicit -n / shorthand / profile volume still wins.
	if mountpoint == "" {
		return nil, fmt.Errorf("mountpoint is required (pass it as an argument or set mount.path in the profile)")
	}

	// Verify that the mountpoint directory exists
	if _, err := os.Stat(mountpoint); os.IsNotExist(err) {
		return nil, fmt.Errorf("mountpoint %s does not exist", mountpoint)
	}

	return &mountTarget{
		cfg:        cfg,
		volume:     volume,
		mountpoint: mountpoint,
		addr:       net.JoinHostPort(cfg.Server.Address, fmt.Sprintf("%d", cfg.Server.Port)),
		password:   v.GetString("auth.password"),
		rawIDs:     rawIDs,
	}, nil
}

// runMount dials the server (following a VolumeService.Resolve referral when
// the volume lives elsewhere), mounts the volume, records the mount state and
// blocks until a signal arrives or the FUSE filesystem is detached
// out-of-band.
func runMount(t *mountTarget) error {
	startPprofIfEnabled()
	startClientMetricsIfEnabled()

	// Fail fast (before minting a client cert / opening the cache) if this
	// machine already has a live gMountie mount at this path — re-mounting the
	// same path otherwise surfaces as an opaque local cache-lock error.
	if st, ok, err := findMountState(t.mountpoint); err == nil && ok && processAlive(st.PID) {
		return fmt.Errorf("%s is already mounted (volume %q, pid %d); unmount it first", t.mountpoint, st.Volume, st.PID)
	}

	// Create client. When t.volume is empty it auto-resolves the sole volume
	// and returns its name, which the mounter/state/logs below then use.
	c, volume, err := grpc.NewClientForVolume(t.cfg, t.volume)
	if err != nil {
		return remediate(err, t.addr, t.volume)
	}

	defer func(c grpc.Client) {
		err := c.Close()
		if err != nil {
			log.Log.Error("failed to close the client", zap.Error(err))
		}
	}(c)

	// Create mounter
	mounter := mount.NewSingleVolumeMounter(c, t.cfg.FUSE, *t.cfg.Cache, t.rawIDs)
	defer func(mounter mount.SingleVolumeMounter) {
		err := mounter.Close()
		if err != nil {
			log.Log.Error("failed to close the mounter", zap.Error(err))
		}
	}(mounter)

	// Mount volume
	if err := mounter.Mount(volume, t.mountpoint); err != nil {
		return remediate(err, t.addr, volume)
	}

	log.Log.Sugar().Infof("Mounted volume %s at %s", volume, t.mountpoint)

	// Record this mount so `gmountie status` can list it and `gmountie
	// unmount` can stop it; clear the record on exit. Best-effort — a
	// state-file failure must not bring down a working mount.
	baseState := mountState{
		Mountpoint: t.mountpoint,
		Server:     t.addr,
		Volume:     volume,
		PID:        os.Getpid(),
		StartedAt:  time.Now(),
		Healthy:    c.SessionLive(),
	}
	if err := writeMountState(baseState); err != nil {
		log.Log.Warn("could not record mount state", zap.Error(err))
	}
	defer func() { _ = removeMountState(t.mountpoint) }()

	// Heartbeat the session-health flag into the record so `gmountie status`
	// (a separate process) can tell a working mount from a zombie whose
	// session is locked out. Stopped explicitly below — before the deferred
	// removeMountState — so a final tick can't resurrect the record.
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		runMountHeartbeat(baseState, c.SessionLive, stopHeartbeat)
	}()

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
		mounter.Wait(volume)
		close(served)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	if waitForUnmount(sig, served) {
		log.Log.Sugar().Infof("Volume %s was unmounted; exiting", volume)
	}

	// Stop the heartbeat before the deferred removeMountState runs, so a final
	// tick can't write the record back after it's deleted (status would then
	// show a mount that's already torn down).
	close(stopHeartbeat)
	<-heartbeatDone

	return nil
}

// runMountHeartbeat refreshes the mount record's Healthy/HeartbeatAt every
// mountHeartbeatInterval until stop is closed. health is the daemon's
// session-liveness probe (grpc.Client.SessionLive). Best-effort: a failed
// write is logged and retried on the next tick — a heartbeat hiccup must never
// affect the running mount. An immediate first beat means status reflects
// health without waiting a full interval.
func runMountHeartbeat(base mountState, health func() bool, stop <-chan struct{}) {
	ticker := time.NewTicker(mountHeartbeatInterval)
	defer ticker.Stop()
	beat := func() {
		rec := base
		rec.Healthy = health()
		rec.HeartbeatAt = time.Now()
		if err := writeMountState(rec); err != nil {
			log.Log.Warn("mount heartbeat write failed", zap.Error(err))
		}
	}
	beat()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			beat()
		}
	}
}

// newMountCmd constructs the mount command with its own flag state.
func newMountCmd() *cobra.Command {
	f := &mountFlags{}
	cmd := &cobra.Command{
		Use:   "mount [user@host[:port]/volume] mountpoint",
		Short: "Mount a gMountie volume",
		Long: "Mount a gMountie volume at the given mountpoint.\n\n" +
			"Shorthand:  gmountie mount admin@host:9449/shared /mnt/shared\n" +
			"Or flags:   gmountie mount /mnt/shared -s host:9449 -n shared -u admin\n\n" +
			"The password is taken from --password, then the config file, then\n" +
			"$GMOUNTIE_AUTH_PASSWORD, then an interactive prompt. Use --daemon to\n" +
			"mount in the background.",
		Args: cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			// In the detached child, recover the password the parent handed over
			// the password pipe (fd 4) before auth is resolved — the child has no
			// TTY to prompt, and the secret was deliberately kept out of its
			// environment (CQ-L2). No-op unless this is the daemon child.
			applyDaemonPassword()

			t, err := buildMountConfig(cmd, args, f)
			if err != nil {
				return err
			}

			// Background mode: hand off to a detached child, passing the resolved
			// password over the password pipe (NOT the environment, which would
			// linger in the child's /proc/<pid>/environ for the mount lifetime).
			if f.daemon {
				return daemonize(execDaemonizer{}, os.Args, t.password)
			}

			// In the detached child, a mount failure before the mount is up
			// returns here without ever signalling the parent. Report the real
			// reason up the ready pipe so the parent prints it instead of a
			// generic timeout (no-op unless this is the daemon child). On
			// success runMount blocks until teardown, so err is nil here.
			err = runMount(t)
			if err != nil {
				signalDaemonError(err)
			}
			return err
		},
	}

	addProfileFlag(cmd)
	cmd.PersistentFlags().StringVarP(&f.serverAddr, "server", "s", "127.0.0.1:9449", "server address")
	cmd.PersistentFlags().StringVarP(&f.volumeName, "volume", "n", "", "volume name")
	addAuthFlags(cmd, &f.auth)
	addCredentialsFlag(cmd)
	addRpcTimeoutFlags(cmd, &f.rpc)
	cmd.PersistentFlags().BoolVar(&f.rawIDs, "raw-ids", false, "expose server-side uids/gids unchanged (for backups/admin tooling)")
	cmd.PersistentFlags().BoolVar(&f.daemon, "daemon", false, "mount in the background (detach after the mount is ready)")
	return cmd
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
	rootCmd.AddCommand(newMountCmd())
}
