package commands

import (
	"fmt"
	"io"
	"os"
)

// passwordEnvVar is the env var checked when no --password flag is given.
const passwordEnvVar = "GMOUNTIE_AUTH_PASSWORD"

// resolvePassword resolves a basic-auth password without putting it on the
// command line: explicit flag value first, then GMOUNTIE_AUTH_PASSWORD, then an
// interactive (no-echo on a TTY) prompt read from `in`. Returns an error if all
// sources are empty (e.g. non-interactive use with nothing supplied).
func resolvePassword(flagValue string, in io.Reader, prompt io.Writer) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if env := os.Getenv(passwordEnvVar); env != "" {
		return env, nil
	}
	read := makePasswordReader(in, prompt)
	pw, err := read("Password: ")
	if err != nil {
		return "", err
	}
	if pw == "" {
		return "", fmt.Errorf("no password provided: pass --password, set %s, or run interactively", passwordEnvVar)
	}
	return pw, nil
}
