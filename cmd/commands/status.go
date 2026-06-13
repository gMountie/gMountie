//go:build linux || darwin

package commands

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "List active gMountie mounts on this machine",
	Long: "Lists the volumes currently mounted by gMountie (foreground and --daemon),\n" +
		"with their mountpoint, server, volume, pid, and uptime. Mounts whose\n" +
		"process has died are pruned. Stop one with `gmountie unmount <mountpoint>`.",
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		states, err := listMountStates()
		if err != nil {
			return fmt.Errorf("read mount state: %w", err)
		}
		renderMountStates(cmd.OutOrStdout(), states)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}

// renderMountStates prints a table of active mounts, or a friendly note when
// there are none.
func renderMountStates(out io.Writer, states []mountState) {
	if len(states) == 0 {
		_, _ = fmt.Fprintln(out, "No active gMountie mounts.")
		return
	}
	now := time.Now()
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "MOUNTPOINT\tVOLUME\tSERVER\tPID\tUPTIME\tSTATUS")
	for _, st := range states {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n",
			st.Mountpoint, st.Volume, st.Server, st.PID, uptime(st.StartedAt), mountStatusOf(st, now))
	}
	_ = tw.Flush()
}

// uptime renders a compact human duration since started (e.g. "1m30s"). Returns
// "-" when the start time is unknown.
func uptime(started time.Time) string {
	if started.IsZero() {
		return "-"
	}
	d := time.Since(started).Round(time.Second)
	if d < 0 {
		d = 0
	}
	return d.String()
}
