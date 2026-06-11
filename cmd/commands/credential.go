//go:build linux || darwin

package commands

import (
	"fmt"
	"net"
	"os"

	"go.gmountie.dev/gmountie/pkg/client/credentials"
	commonconfig "go.gmountie.dev/gmountie/pkg/common/config"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// credentialsFile is set by the --credentials flag on whichever client command
// runs (mount/ls). It points at a single-blob mount credential; when empty the
// $GMOUNTIE_CREDENTIALS env var is consulted instead.
var credentialsFile string

// addCredentialsFlag registers --credentials on a command. Shared by mount and
// ls so both can mount/list from a single credential blob with cert-only auth.
func addCredentialsFlag(cmd *cobra.Command) {
	cmd.Flags().StringVar(&credentialsFile, "credentials", "",
		"path to a single-blob mount credential; $GMOUNTIE_CREDENTIALS is used when unset")
}

// applyCredential maps a decoded credential blob onto the viper instance:
// the data-plane endpoint, the inline TLS material (data CA + this device's
// client cert/key), the verification name, and auth.type=mtls; or, in token
// mode, the renew endpoint/token that configure the certificate refresher.
//
// It sits ABOVE the config file but BELOW explicit CLI flags: it runs after the
// config-file read, and the few flags that map to the same keys (-s, -u, -t,
// --verify-style options) are layered afterwards and still win. verify defaults
// to "verify" (full chain validation) only when the config file did not set it.
func applyCredential(v *viper.Viper, cred *credentials.Credentials) error {
	host, port, err := net.SplitHostPort(cred.Endpoint)
	if err != nil {
		return fmt.Errorf("invalid credential endpoint %q: %w", cred.Endpoint, err)
	}
	v.Set("server.address", host)
	v.Set("server.port", port)
	if cred.ServerName != "" {
		v.Set("server.tls.server_name", cred.ServerName)
	}
	if cred.TokenMode() {
		v.Set("renew.endpoint", cred.RenewEndpoint)
		v.Set("renew.token", cred.RenewToken)
		// CA + cert identity arrive via the refresher's profile exchange;
		// no static TLS material to set.
	} else {
		v.Set("server.tls.ca_pem", cred.CAPEM)
		v.Set("server.tls.cert_pem", cred.CertPEM)
		v.Set("server.tls.key_pem", cred.KeyPEM)
	}
	if !v.IsSet("server.tls.verify") {
		v.Set("server.tls.verify", "verify")
	}
	v.Set("auth.type", string(commonconfig.AuthConfigTypeMTLS))
	return nil
}

// applyCredentialToViper loads a single-blob credential (from --credentials or
// $GMOUNTIE_CREDENTIALS) and, when present, applies it onto v via applyCredential.
// It returns whether a credential was applied so callers can guard flag defaults
// (e.g. the mount -s override) that would otherwise clobber the credential's
// endpoint. No credential is not an error — (false, nil) means "fall back to the
// config file / flags".
func applyCredentialToViper(v *viper.Viper) (bool, error) {
	cred, err := credentials.Load(os.Getenv(credentialsEnvVar), credentialsFile)
	if err != nil {
		return false, fmt.Errorf("failed to load credentials: %w", err)
	}
	if cred == nil {
		return false, nil
	}
	if err := applyCredential(v, cred); err != nil {
		return false, err
	}
	return true, nil
}
