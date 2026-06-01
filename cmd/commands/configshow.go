package commands

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	commonconfig "go.gmountie.dev/gmountie/pkg/common/config"

	"github.com/spf13/cobra"
)

var (
	configShowFor       string
	configShowEffective bool
)

// secretKeyLine matches a YAML assignment for a sensitive key, capturing its
// prefix (indent plus an optional "- " sequence-item marker), key name, and
// whatever follows the colon. `key_pem` is the inline mTLS private key;
// `password`/`password_hash` are basic-auth secrets. Public material
// (`ca_pem`, `cert_pem`) and file-path references are intentionally left
// visible. Listed longest-first so `password_hash` wins over `password`. The
// optional "- " matters because a secret that is the first key of a list item
// (e.g. server auth.users[].password_hash) renders as `- password_hash: ...`.
var secretKeyLine = regexp.MustCompile(`^(\s*(?:- )?)(password_hash|password|key_pem)\s*:\s*(.*)$`)

// blockScalarIndicator matches a YAML block-scalar header (`|`, `>`, with an
// optional chomping/indentation modifier and an optional trailing comment),
// meaning the value lives on the indented lines that follow rather than on the
// key line itself. The trailing-comment case (`key_pem: | # note`) must match
// or the indented secret body would be mistaken for an inline value and leak.
var blockScalarIndicator = regexp.MustCompile(`^[|>][+-]?\d*(\s+#.*)?$`)

// redactConfigYAML replaces sensitive values with REDACTED, leaving structure
// and non-secret values intact. It handles both inline scalars and block
// scalars (`key_pem: |` followed by indented PEM lines), collapsing a redacted
// block to a single inline REDACTED so no secret bytes survive.
func redactConfigYAML(in string) string {
	lines := strings.Split(in, "\n")
	out := make([]string, 0, len(lines))
	for i := 0; i < len(lines); i++ {
		m := secretKeyLine.FindStringSubmatch(lines[i])
		if m == nil {
			out = append(out, lines[i])
			continue
		}
		indent, key, value := m[1], m[2], strings.TrimSpace(m[3])
		out = append(out, indent+key+": REDACTED")
		if value == "" || blockScalarIndicator.MatchString(value) {
			// Block scalar (or dangling key): drop the indented body that
			// carries the secret. The body is the run of following non-blank
			// lines indented deeper than the key; a blank line or a dedent
			// ends it (PEM bodies contain no blank lines).
			for i+1 < len(lines) {
				next := lines[i+1]
				if strings.TrimSpace(next) == "" || leadingSpaces(next) <= len(indent) {
					break
				}
				i++
			}
		}
	}
	return strings.Join(out, "\n")
}

// leadingSpaces returns the number of leading space characters. YAML forbids
// tabs for indentation, so counting spaces is sufficient.
func leadingSpaces(s string) int {
	return len(s) - len(strings.TrimLeft(s, " "))
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Inspect gMountie configuration",
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Print a gMountie config file with secrets redacted",
	Long: "Reads the config file (--config, or the default for --for) and prints it\n" +
		"verbatim with secrets redacted — so you can confirm which file gMountie\n" +
		"will load and what it contains. Fields you omit are not shown here; they\n" +
		"fall back to the documented defaults (see the client/server config docs).\n\n" +
		"Use --effective to instead print the resolved configuration (your file\n" +
		"merged with the built-in defaults and GMOUNTIE_* environment overrides),\n" +
		"so you can see every value gMountie will actually use. With --effective,\n" +
		"pass --for server to render a server config (it defaults to client).",
	RunE: runConfigShow,
}

func init() {
	configShowCmd.Flags().StringVar(&configShowFor, "for", "", "which config to show when no --config is given: server|client")
	configShowCmd.Flags().BoolVar(&configShowEffective, "effective", false, "print the resolved config (file + defaults + env), not the verbatim file")
	addProfileFlag(configShowCmd)
	configCmd.AddCommand(configShowCmd)
	rootCmd.AddCommand(configCmd)
}

// resolveConfigShowPath picks the config file to show: --profile, then
// --config, then the per-role default for --for.
func resolveConfigShowPath() (string, error) {
	profilePath, err := resolveProfilePath()
	if err != nil {
		return "", err
	}
	if profilePath != "" {
		return profilePath, nil
	}
	if configFile != "" {
		return configFile, nil
	}
	switch configShowFor {
	case "server":
		return commonconfig.GetDefaultConfigPath(commonconfig.DefaultServerConfigFileName), nil
	case "client", "":
		return commonconfig.GetDefaultConfigPath(commonconfig.DefaultClientConfigFileName), nil
	default:
		return "", fmt.Errorf("--for must be server or client, got %q", configShowFor)
	}
}

func runConfigShow(cmd *cobra.Command, _ []string) error {
	path, err := resolveConfigShowPath()
	if err != nil {
		return err
	}
	if configShowEffective {
		rendered, err := renderEffectiveConfig(path, configShowFor)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "# %s (effective: file + defaults + env)\n%s\n", path, rendered)
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "# %s\n%s\n", path, redactConfigYAML(string(data)))
	return nil
}
