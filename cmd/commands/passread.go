package commands

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
)

// makePasswordReader returns a closure that reads one password per call.
// When stdin is a real TTY, terminal echo is suppressed via term.ReadPassword.
// Otherwise (tests piping input, scripted use) a shared bufio.Reader reads one
// line per call so that "pw1\npw2\n" on a pipe yields "pw1" then "pw2".
// Shared by genpass, mount, and ls.
func makePasswordReader(in io.Reader, prompt io.Writer) func(label string) (string, error) {
	if f, ok := in.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		return func(label string) (string, error) {
			_, _ = fmt.Fprint(prompt, label)
			buf, err := term.ReadPassword(int(f.Fd()))
			if err != nil {
				return "", err
			}
			_, _ = fmt.Fprintln(prompt)
			return string(buf), nil
		}
	}
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
