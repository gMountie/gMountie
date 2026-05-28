package commands

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"gmountie/pkg/common/passhash"

	"github.com/spf13/cobra"
	"golang.org/x/term"
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

// makePasswordReader returns a closure that reads one password per call.
// When stdin is a real TTY, terminal echo is suppressed via term.ReadPassword.
// Otherwise (tests piping input, scripted use) a shared bufio.Reader reads one
// line per call so that "pw1\npw2\n" on a pipe yields "pw1" then "pw2"
// without over-read confusion from creating two independent bufio.Readers.
func makePasswordReader(in io.Reader, prompt io.Writer) func(label string) (string, error) {
	if f, ok := in.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		return func(label string) (string, error) {
			_, _ = fmt.Fprint(prompt, label)
			buf, err := term.ReadPassword(int(f.Fd()))
			if err != nil {
				return "", err
			}
			_, _ = fmt.Fprintln(prompt) // restore cursor to next line after no-echo input
			return string(buf), nil
		}
	}
	// Non-TTY: share a single bufio.Reader across both calls so the second
	// read starts exactly where the first left off in the underlying stream.
	br := bufio.NewReader(in)
	return func(label string) (string, error) {
		_, _ = fmt.Fprint(prompt, label)
		line, err := br.ReadString('\n')
		if err != nil && (err != io.EOF || line == "") {
			return "", err
		}
		return strings.TrimRight(line, "\r\n"), nil
	}
}
