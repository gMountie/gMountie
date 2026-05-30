package commands

import (
	"fmt"
	"os"
	"regexp"

	commonconfig "gmountie/pkg/common/config"

	"github.com/spf13/cobra"
)

var configShowFor string

// secretLine matches a YAML scalar assignment for a sensitive key so its value
// can be replaced regardless of nesting/indentation.
var secretLine = regexp.MustCompile(`(?m)^(\s*(?:password|password_hash)\s*:\s*).+$`)

// redactConfigYAML replaces password / password_hash values with REDACTED,
// leaving structure and non-secret values intact.
func redactConfigYAML(in string) string {
	return secretLine.ReplaceAllString(in, "${1}REDACTED")
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Inspect gMountie configuration",
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Print the effective configuration (secrets redacted)",
	Long: "Reads the config file (--config, or the default for --for) and prints it\n" +
		"with passwords redacted, so you can see what gMountie would load.",
	RunE: runConfigShow,
}

func init() {
	configShowCmd.Flags().StringVar(&configShowFor, "for", "", "which config to show when no --config is given: server|client")
	configCmd.AddCommand(configShowCmd)
	rootCmd.AddCommand(configCmd)
}

func runConfigShow(cmd *cobra.Command, _ []string) error {
	path := configFile
	if path == "" {
		switch configShowFor {
		case "server":
			path = commonconfig.GetDefaultConfigPath(commonconfig.DefaultServerConfigFileName)
		case "client", "":
			path = commonconfig.GetDefaultConfigPath(commonconfig.DefaultClientConfigFileName)
		default:
			return fmt.Errorf("--for must be server or client, got %q", configShowFor)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "# %s\n%s\n", path, redactConfigYAML(string(data)))
	return nil
}
