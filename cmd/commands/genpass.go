package commands

import (
	"errors"
	"fmt"

	"gmountie/pkg/common/passhash"

	"github.com/spf13/cobra"
)

var genpassCmd = &cobra.Command{
	Use:   "genpass",
	Short: "Generate an argon2id password hash for use in basic-auth config",
	Long: "Reads a password from stdin (or terminal — no echo), confirms it,\n" +
		"and prints the argon2id PHC string on stdout. Paste the output into\n" +
		"the server config under auth.users[].password_hash.",
	RunE: runGenpass,
}

func init() {
	rootCmd.AddCommand(genpassCmd)
}

func runGenpass(cmd *cobra.Command, _ []string) error {
	stderr := cmd.ErrOrStderr()
	stdout := cmd.OutOrStdout()
	stdin := cmd.InOrStdin()

	read := makePasswordReader(stdin, stderr)

	pw1, err := read("Password: ")
	if err != nil {
		return err
	}
	if pw1 == "" {
		return errors.New("password required")
	}

	pw2, err := read("Confirm:  ")
	if err != nil {
		return err
	}
	if pw1 != pw2 {
		return errors.New("passwords do not match")
	}

	phc, err := passhash.Hash(pw1)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	_, _ = fmt.Fprintln(stdout, phc)
	return nil
}
