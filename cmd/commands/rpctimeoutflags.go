//go:build linux || darwin

package commands

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// rpcTimeoutFlags holds the optional per-RPC timeout overrides shared by mount
// and ls. A zero value means "not set" — the config file or built-in defaults
// stand; only a flag the user actually passed is applied.
type rpcTimeoutFlags struct {
	meta time.Duration
	io   time.Duration
}

// addRpcTimeoutFlags registers --rpc-timeout-meta and --rpc-timeout-io. They map
// onto rpc.timeout_meta / rpc.timeout_io, letting a user widen the
// connect/resolve/metadata budget (meta) or the data read/write budget (io) on a
// high-latency link without writing a config file — the remedy the resolve
// timeout error points at. Shared by mount and ls: both open a pre-session
// connection bounded by the meta timeout.
func addRpcTimeoutFlags(cmd *cobra.Command, f *rpcTimeoutFlags) {
	cmd.Flags().DurationVar(&f.meta, "rpc-timeout-meta", 0,
		"per-RPC timeout for metadata calls incl. connect/resolve, e.g. 10s (overrides rpc.timeout_meta)")
	cmd.Flags().DurationVar(&f.io, "rpc-timeout-io", 0,
		"per-RPC timeout for data read/write calls, e.g. 60s (overrides rpc.timeout_io)")
}

// applyRpcTimeoutFlags pushes any explicitly-set timeout flag onto viper, above
// the config file (matching the credential/auth precedence). An unset flag is
// skipped so it never clobbers a config value with the zero default.
func applyRpcTimeoutFlags(cmd *cobra.Command, v *viper.Viper, f *rpcTimeoutFlags) {
	if cmd.Flags().Changed("rpc-timeout-meta") {
		v.Set("rpc.timeout_meta", f.meta)
	}
	if cmd.Flags().Changed("rpc-timeout-io") {
		v.Set("rpc.timeout_io", f.io)
	}
}
