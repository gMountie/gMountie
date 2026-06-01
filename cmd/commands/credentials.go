package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/viper"
)

// passwordEnvVar is the env var checked when no --password flag is given.
const passwordEnvVar = "GMOUNTIE_AUTH_PASSWORD"

// resolveConfiguredPassword resolves a password from the config (profile/file)
// sources, in order: auth.password_command (run via `sh -c`), then
// auth.password_file (or $GMOUNTIE_AUTH_PASSWORD_FILE), then the inline
// auth.password. Returns "" with no error when none are set, so the caller can
// fall back to the flag/env/prompt chain.
func resolveConfiguredPassword(ctx context.Context, v *viper.Viper) (string, error) {
	if cmd := strings.TrimSpace(v.GetString("auth.password_command")); cmd != "" {
		out, err := exec.CommandContext(ctx, "sh", "-c", cmd).Output()
		if err != nil {
			return "", fmt.Errorf("auth.password_command failed: %w", err)
		}
		return strings.TrimRight(string(out), "\r\n"), nil
	}
	file := v.GetString("auth.password_file")
	if file == "" {
		file = os.Getenv("GMOUNTIE_AUTH_PASSWORD_FILE")
	}
	if file != "" {
		info, err := os.Stat(file)
		if err != nil {
			return "", fmt.Errorf("auth.password_file: %w", err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			return "", fmt.Errorf("auth.password_file %s is too permissive (%o); use 0600", file, info.Mode().Perm())
		}
		b, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("auth.password_file: %w", err)
		}
		return strings.TrimRight(string(b), "\r\n"), nil
	}
	return v.GetString("auth.password"), nil
}

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
