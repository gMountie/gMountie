package commands

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	servertls "gmountie/pkg/server/tls"

	"github.com/adrg/xdg"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var fingerprintVerbose bool

var fingerprintCmd = &cobra.Command{
	Use:   "fingerprint",
	Short: "Print the server certificate fingerprint",
	Long: "Resolves the cert path the server would present and prints its SHA-256\n" +
		"fingerprint in SSH form (SHA256:<base64>). One line by default; --verbose\n" +
		"adds subject, issuer, and validity dates. Read-only: never auto-generates.",
	RunE: runFingerprint,
}

func init() {
	fingerprintCmd.Flags().BoolVar(&fingerprintVerbose, "verbose", false,
		"print subject, issuer, NotBefore, NotAfter alongside the fingerprint")
	rootCmd.AddCommand(fingerprintCmd)
}

func runFingerprint(cmd *cobra.Command, _ []string) error {
	certPath, fromConfig := resolveCertPath()
	pemBytes, err := servertls.LoadCertOnly(certPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			if fromConfig {
				return fmt.Errorf("no server cert at %s; set server.tls.cert_file to an existing file or run 'gmountie serve' to auto-generate", certPath)
			}
			return fmt.Errorf("no server cert at %s; run 'gmountie serve' once to auto-generate, or set server.tls.cert_file", certPath)
		}
		return err
	}
	fp, err := servertls.Fingerprint(pemBytes)
	if err != nil {
		return fmt.Errorf("compute fingerprint of %s: %w", certPath, err)
	}

	out := cmd.OutOrStdout()
	if !fingerprintVerbose {
		_, _ = fmt.Fprint(out, renderFingerprint(fp))
		return nil
	}

	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return fmt.Errorf("decode PEM at %s", certPath)
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse cert at %s: %w", certPath, err)
	}
	_, _ = fmt.Fprintf(out, "Path:        %s\n", certPath)
	_, _ = fmt.Fprintf(out, "Subject:     %s\n", c.Subject)
	_, _ = fmt.Fprintf(out, "Issuer:      %s\n", c.Issuer)
	_, _ = fmt.Fprintf(out, "NotBefore:   %s\n", c.NotBefore.UTC().Format("2006-01-02 15:04:05 UTC"))
	_, _ = fmt.Fprintf(out, "NotAfter:    %s\n", c.NotAfter.UTC().Format("2006-01-02 15:04:05 UTC"))
	_, _ = fmt.Fprintf(out, "Fingerprint: %s\n", fp)
	return nil
}

// renderFingerprint formats the one-line (non-verbose) output: the raw
// fingerprint plus a copy-paste-ready client config snippet.
func renderFingerprint(fp string) string {
	return fmt.Sprintf("%s\n\n# Add to client.yaml under server.tls:\n#   verify: tofu\n#   expected_fingerprint: %s\n", fp, fp)
}

// resolveCertPath returns the cert path the command will read, and a flag
// indicating whether the path came from explicit config (so we can pick the
// right error wording when it's missing). server.tls.cert_file wins; falls
// back to $XDG_STATE_HOME/gmountie/server.crt.
func resolveCertPath() (path string, fromConfig bool) {
	if p := viper.GetString("server.tls.cert_file"); p != "" {
		return p, true
	}
	// xdg.StateFile resolves $XDG_STATE_HOME/<relative>, creating parent dirs
	// — but for a read-only resolve we just want the path string. Compose it
	// manually so we don't side-effect.
	return filepath.Join(xdg.StateHome, "gmountie", "server.crt"), false
}
