# CLI/Config UX Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make gMountie's CLI braindead-simple: a zero-edit first-run server, an sshfs-style one-line mount, credentials that never sit on the command line, plus the lifecycle/discovery commands users expect.

**Architecture:** Pure, table-tested helpers (password reader, passphrase generator, mount-spec parser, error classifier, daemon arg-builder) live in small focused files under `cmd/commands/` and `pkg/common/`; the cobra commands stay thin wiring over them. No proto or wire-protocol changes — `gmountie ls` reuses the existing `VolumeService.List` RPC.

**Tech Stack:** Go, cobra, viper, `go-fuse`, gRPC, testify suites, `golang.org/x/term`, `adrg/xdg`, argon2id (`pkg/common/passhash`).

**Conventions (from project memory — follow exactly):**
- Tests are methods on a testify `suite.Suite`, not bare `func TestX`. Each file ends with one `func TestXxxSuite(t *testing.T) { suite.Run(t, new(XxxSuite)) }`.
- Commit autonomously after each task: conventional-commit subject + a short descriptive body (mandatory). No `Co-Authored-By` / `Signed-off-by` trailers.
- No backwards-compatibility design; document behavior breaks in release notes (Task 15).
- Logging via `gmountie/pkg/utils/log` (`log.Log`), errors wrapped with `github.com/pkg/errors`.
- Run the full suite with `go test -failfast ./...`. A single suite: `go test -v -run TestXxxSuite ./cmd/commands/...`.
- FUSE-mount e2e is env-gated and validated on the kubevirt VM, not the sandbox — do not run real-mount tests locally.

**Verified signatures this plan depends on:**
- `passhash.Hash(string) (string, error)`, `passhash.IsHashed(string) bool` — `pkg/common/passhash/argon2id.go:39,102`.
- `config.WriteDefaultConfig(name, content string) error`, `config.EnsureConfigDir() error`, `config.GetDefaultConfigPath(name) string`, `config.DefaultServerConfigFileName` — `pkg/common/config/paths.go`.
- Server `config.Config{ Server, Auth, Volumes []*VolumeConfig, Log }`; `VolumeConfig{ Name, Path string }` — `pkg/server/config/{config.go:19,volumes.go:43}`.
- `server.Start(ctx, *config.Config) error` — `pkg/server/app.go:102`.
- Client `config.Config{ Server *ServerConfig, Auth, Mount, Rpc, FUSE, Cache, Log }`; `ServerConfig{ Address string, Port uint, Endpoint string, TLS }` — `pkg/client/config/{config.go:19,server.go:26}`. `config.ParseConfig(*viper.Viper) (*Config, error)`.
- `grpc.NewClientFromConfig(*config.Config) (grpc.Client, error)`; `Client.Volume() proto.VolumeServiceClient`, `Client.Close() error` — `pkg/client/grpc/client.go:20,205`.
- `proto.VolumeServiceClient.List(ctx, *VolumeListRequest, ...) (*VolumeListReply, error)`; `VolumeListReply.GetVolumes() []*proto.Volume`; `Volume.GetName() string` — `pkg/proto/volume*.go`.
- `makePasswordReader(in io.Reader, prompt io.Writer) func(label string) (string, error)` currently in `cmd/commands/genpass.go:66`.

---

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `cmd/commands/passread.go` | Create | Move `makePasswordReader` here (shared by genpass/mount/ls) |
| `cmd/commands/genpass.go` | Modify | Drop local `makePasswordReader` (now in passread.go) |
| `pkg/common/passhash/genphrase.go` | Create | `GeneratePassphrase()` crypto-random base32 passphrase |
| `cmd/commands/serve.go` | Modify | First-run: random pw, default `shared` volume, `0.0.0.0`, create data dir |
| `pkg/server/config/validate.go` | Create | `(*Config).ValidateVolumePaths()` — paths exist & are dirs |
| `pkg/server/app.go` | Modify | Call `ValidateVolumePaths()` early in `Start` |
| `cmd/commands/mountspec.go` | Create | `parseMountSpec` pure parser for `[user@]host[:port]/volume` |
| `cmd/commands/credentials.go` | Create | `resolvePassword` (flag → env → TTY prompt → error) |
| `cmd/commands/clienterr.go` | Create | `remediate(err)` connect/auth/volume hint classifier |
| `cmd/commands/daemon.go` | Create | `buildDaemonChildArgs` + `daemonizer` seam + default spawner |
| `cmd/commands/mount.go` | Modify | Shorthand args, password resolver, `--daemon`, remediation |
| `cmd/commands/ls.go` | Create | `gmountie ls [user@]host[:port]` |
| `cmd/commands/configshow.go` | Create | `gmountie config show [--for server|client]` |
| `cmd/commands/fingerprint.go` | Modify | Append paste-ready `expected_fingerprint:` snippet |
| `cmd/commands/root.go` | Modify | Richer `--config`/`--verbose` help |
| `packaging/systemd/gmountie-serve.service` | Create | server unit |
| `packaging/systemd/gmountie-mount@.service` | Create | per-mount template unit |
| `website/docs/quickstart.*`, `README.md`, `docs/design/security-and-transport.md` | Modify | Docs |
| `docs/RELEASE_NOTES_cli-ux.md` | Create | Behavior-break notes |

Each `*_test.go` sits beside its source file.

---

## Task 1: Extract shared password reader

**Files:**
- Create: `cmd/commands/passread.go`
- Modify: `cmd/commands/genpass.go:61-89` (remove the function)
- Test: `cmd/commands/passread_test.go`

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type PassReadSuite struct{ suite.Suite }

func TestPassReadSuite(t *testing.T) { suite.Run(t, new(PassReadSuite)) }

func (s *PassReadSuite) TestNonTTYReadsLinesInOrder() {
	var prompt bytes.Buffer
	read := makePasswordReader(strings.NewReader("first\nsecond\n"), &prompt)

	pw1, err := read("Password: ")
	s.Require().NoError(err)
	s.Equal("first", pw1)

	pw2, err := read("Confirm:  ")
	s.Require().NoError(err)
	s.Equal("second", pw2)
	s.Contains(prompt.String(), "Password: ")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestPassReadSuite ./cmd/commands/`
Expected: COMPILE FAIL or PASS-but-duplicate — at this point `makePasswordReader` still lives in `genpass.go`, so the test compiles and passes. That's fine; the point of this task is the *move*. Proceed to make the move and keep it green.

- [ ] **Step 3: Create `passread.go` with the moved function**

Cut the `makePasswordReader` function (lines 61-89) and its required imports (`bufio`, `io`, `os`, `strings`, `golang.org/x/term`, `fmt`) out of `genpass.go` into a new file:

```go
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
```

- [ ] **Step 4: Remove the function from `genpass.go`**

Delete lines 61-89 from `genpass.go`. Then prune `genpass.go`'s imports to only what it still uses: `errors`, `fmt`, `gmountie/pkg/common/passhash`, `github.com/spf13/cobra`. (Remove `bufio`, `io`, `os`, `strings`, `golang.org/x/term`.)

- [ ] **Step 5: Run tests + build**

Run: `go test -v -run 'TestPassReadSuite|Genpass' ./cmd/commands/ && go build ./...`
Expected: PASS, build clean (no unused-import errors).

- [ ] **Step 6: Commit**

```bash
git add cmd/commands/passread.go cmd/commands/passread_test.go cmd/commands/genpass.go
git commit -m "refactor(cli): extract shared password reader

Move makePasswordReader out of genpass into passread.go so mount and ls
can reuse the no-echo TTY reader."
```

---

## Task 2: Random passphrase generator

**Files:**
- Create: `pkg/common/passhash/genphrase.go`
- Test: `pkg/common/passhash/genphrase_test.go`

- [ ] **Step 1: Write the failing test**

```go
package passhash

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type GenPhraseSuite struct{ suite.Suite }

func TestGenPhraseSuite(t *testing.T) { suite.Run(t, new(GenPhraseSuite)) }

func (s *GenPhraseSuite) TestGeneratesUsableDistinctPassphrases() {
	a, err := GeneratePassphrase()
	s.Require().NoError(err)
	b, err := GeneratePassphrase()
	s.Require().NoError(err)

	s.GreaterOrEqual(len(a), 20, "passphrase should be at least 20 chars")
	s.NotEqual(a, b, "two calls must differ (crypto-random)")
	s.NotContains(a, "=", "no base32 padding")

	// It must round-trip through the real hash+verify path.
	phc, err := Hash(a)
	s.Require().NoError(err)
	ok, err := Verify(phc, a)
	s.Require().NoError(err)
	s.True(ok)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestGenPhraseSuite ./pkg/common/passhash/`
Expected: FAIL — `undefined: GeneratePassphrase`.

- [ ] **Step 3: Implement the generator**

```go
package passhash

import (
	"crypto/rand"
	"encoding/base32"

	"github.com/pkg/errors"
)

// passphraseBytes is the entropy size; 15 bytes -> 24 base32 chars (no padding).
const passphraseBytes = 15

// GeneratePassphrase returns a crypto-random, human-transcribable passphrase
// (lowercase base32, no padding). Used for the first-run server admin password
// so we never ship a fixed credential.
func GeneratePassphrase() (string, error) {
	buf := make([]byte, passphraseBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", errors.Wrap(err, "read random bytes")
	}
	enc := base32.StdEncoding.WithPadding(base32.NoPadding)
	return strings.ToLower(enc.EncodeToString(buf)), nil
}
```

Add `"strings"` to the import block (used by `strings.ToLower`).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestGenPhraseSuite ./pkg/common/passhash/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/common/passhash/genphrase.go pkg/common/passhash/genphrase_test.go
git commit -m "feat(passhash): add GeneratePassphrase

Crypto-random lowercase base32 passphrase (24 chars) for the first-run
server password so no fixed credential ships."
```

---

## Task 3: Frictionless first-run server config

**Files:**
- Modify: `cmd/commands/serve.go:22-101`
- Test: `cmd/commands/serve_test.go` (add suite methods)

Replace the fixed-`admin` template with one that binds `0.0.0.0`, carries a real
`shared` volume, and uses a freshly generated password (hashed in the file,
plaintext printed once). Create the volume's data dir before writing the config.

- [ ] **Step 1: Write the failing test**

```go
func (s *ServeSuite) TestFirstRunConfigIsUsable() {
	dataDir := s.T().TempDir()
	pw, cfgYAML, err := buildFirstRunConfig(dataDir)
	s.Require().NoError(err)

	s.NotEqual("admin", pw, "must not ship the fixed admin password")
	s.GreaterOrEqual(len(pw), 20)
	s.Contains(cfgYAML, "address: 0.0.0.0")
	s.Contains(cfgYAML, "name: shared")
	s.Contains(cfgYAML, dataDir)
	s.NotContains(cfgYAML, pw, "plaintext password must not be written to the file")

	// The generated config must parse and validate.
	cfg, err := serverConfig.LoadConfigFromString(cfgYAML)
	s.Require().NoError(err)
	s.Require().Len(cfg.Volumes, 1)
	s.Equal("shared", cfg.Volumes[0].Name)
}
```

(Assumes a `ServeSuite` exists in `serve_test.go`; if not, add the standard
`type ServeSuite struct{ suite.Suite }` + `func TestServeSuite(t *testing.T){ suite.Run(t, new(ServeSuite)) }`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestServeSuite ./cmd/commands/`
Expected: FAIL — `undefined: buildFirstRunConfig`.

- [ ] **Step 3: Implement `buildFirstRunConfig` and rewrite the template**

In `serve.go`, replace the template + `DefaultConfig` var (lines 22-47) with:

```go
// firstRunConfigTemplate is the first-run server config. %q is the volume data
// directory; the trailing %s is a freshly-hashed admin password. Binds 0.0.0.0
// so the server is reachable remotely out of the box (random password + auto
// TLS keep that safe); replace the hash via 'gmountie genpass' to rotate.
const firstRunConfigTemplate = `server:
  # 0.0.0.0 accepts remote connections. Set to 127.0.0.1 to restrict to localhost.
  address: 0.0.0.0
  port: 9449
  metrics: true

auth:
  type: basic
  users:
    - username: admin
      # Replace with the output of: gmountie genpass
      password_hash: %s

volumes:
  # Add or edit volumes here. Each exposes a server directory under a name.
  - name: shared
    path: %q
`

// buildFirstRunConfig generates the first-run server config. It returns the
// generated plaintext password (to print once), the YAML to write, and any
// error. The volume data dir is created (0700) by the caller before serving.
func buildFirstRunConfig(dataDir string) (plaintext, yaml string, err error) {
	plaintext, err = passhash.GeneratePassphrase()
	if err != nil {
		return "", "", fmt.Errorf("generate admin password: %w", err)
	}
	hash, err := passhash.Hash(plaintext)
	if err != nil {
		return "", "", fmt.Errorf("hash admin password: %w", err)
	}
	return plaintext, fmt.Sprintf(firstRunConfigTemplate, hash, dataDir), nil
}

// defaultVolumeDir is the auto-created data directory for the first-run
// "shared" volume: $XDG_DATA_HOME/gmountie/shared.
func defaultVolumeDir() string {
	return filepath.Join(xdg.DataHome, "gmountie", "shared")
}
```

Update `serve.go` imports: add `"path/filepath"` and `"github.com/adrg/xdg"`; remove the now-unused `passhash`-only `DefaultConfig` references. Keep `passhash`.

- [ ] **Step 4: Run the helper test to verify it passes**

Run: `go test -v -run TestServeSuite ./cmd/commands/`
Expected: PASS.

- [ ] **Step 5: Wire the helper into the first-run branch of `serveCmd.RunE`**

Replace the "Config doesn't exist, create default one" branch (serve.go ~73-85) with:

```go
			// Config doesn't exist — generate a usable first-run config.
			log.Log.Info("no config file found, creating default configuration",
				zap.String("path", configFile))

			if err := config.EnsureConfigDir(); err != nil {
				return err
			}

			dataDir := defaultVolumeDir()
			if err := os.MkdirAll(dataDir, 0o700); err != nil {
				return fmt.Errorf("create default volume dir %s: %w", dataDir, err)
			}

			plaintext, generated, err := buildFirstRunConfig(dataDir)
			if err != nil {
				return err
			}
			if err := config.WriteDefaultConfig(config.DefaultServerConfigFileName, generated); err != nil {
				return err
			}

			// Print the generated password once — it is never stored in plaintext.
			fmt.Fprintf(cmd.ErrOrStderr(),
				"\n  ┌─ gMountie first run ───────────────────────────────\n"+
					"  │ A server config was created at:\n  │   %s\n"+
					"  │ Exposing volume \"shared\" at: %s\n"+
					"  │\n"+
					"  │ Login:    admin\n"+
					"  │ Password: %s\n"+
					"  │ (shown only now — save it; rotate with `gmountie genpass`)\n"+
					"  └────────────────────────────────────────────────────\n\n",
				configFile, dataDir, plaintext)

			cfgString = generated
```

- [ ] **Step 6: Run full package tests + build**

Run: `go test ./cmd/commands/... && go build ./...`
Expected: PASS, clean build.

- [ ] **Step 7: Commit**

```bash
git add cmd/commands/serve.go cmd/commands/serve_test.go
git commit -m "feat(serve): usable zero-edit first run

Generate a random admin password (printed once, hashed on disk), ship a
working 'shared' volume backed by an auto-created data dir, and bind
0.0.0.0 so the server is reachable remotely without editing config."
```

---

## Task 4: Startup volume-path validation

**Files:**
- Create: `pkg/server/config/validate.go`
- Modify: `pkg/server/app.go:102-114` (call early in `Start`)
- Test: `pkg/server/config/validate_test.go`

- [ ] **Step 1: Write the failing test**

```go
package config

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ValidatePathsSuite struct{ suite.Suite }

func TestValidatePathsSuite(t *testing.T) { suite.Run(t, new(ValidatePathsSuite)) }

func (s *ValidatePathsSuite) TestAcceptsExistingDir() {
	dir := s.T().TempDir()
	c := &Config{Volumes: []*VolumeConfig{{Name: "shared", Path: dir}}}
	s.NoError(c.ValidateVolumePaths())
}

func (s *ValidatePathsSuite) TestRejectsMissingPath() {
	missing := filepath.Join(s.T().TempDir(), "nope")
	c := &Config{Volumes: []*VolumeConfig{{Name: "shared", Path: missing}}}
	err := c.ValidateVolumePaths()
	s.Require().Error(err)
	s.Contains(err.Error(), "shared")
	s.Contains(err.Error(), missing)
}

func (s *ValidatePathsSuite) TestRejectsFileNotDir() {
	f := filepath.Join(s.T().TempDir(), "afile")
	s.Require().NoError(os.WriteFile(f, []byte("x"), 0o600))
	c := &Config{Volumes: []*VolumeConfig{{Name: "v", Path: f}}}
	err := c.ValidateVolumePaths()
	s.Require().Error(err)
	s.Contains(err.Error(), "not a directory")
}
```

Add `"os"` to the test imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestValidatePathsSuite ./pkg/server/config/`
Expected: FAIL — `c.ValidateVolumePaths undefined`.

- [ ] **Step 3: Implement validation**

```go
package config

import (
	"os"

	"github.com/pkg/errors"
)

// ValidateVolumePaths checks that every configured volume path exists and is a
// directory. Called at startup so misconfiguration fails fast with a clear
// message instead of surfacing as a cryptic error on the first I/O.
func (c *Config) ValidateVolumePaths() error {
	for _, v := range c.Volumes {
		info, err := os.Stat(v.Path)
		if err != nil {
			if os.IsNotExist(err) {
				return errors.Errorf("volume %q: path does not exist: %s", v.Name, v.Path)
			}
			return errors.Wrapf(err, "volume %q: stat %s", v.Name, v.Path)
		}
		if !info.IsDir() {
			return errors.Errorf("volume %q: path is not a directory: %s", v.Name, v.Path)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestValidatePathsSuite ./pkg/server/config/`
Expected: PASS.

- [ ] **Step 5: Call it early in `Start`**

In `pkg/server/app.go`, immediately after the `cfg.Log` reconfigure block (after line 109, before `NewServerAppContext`):

```go
	if err := cfg.ValidateVolumePaths(); err != nil {
		return errors.Wrap(err, "invalid volume configuration")
	}
```

(`errors` from `github.com/pkg/errors` is already imported in app.go.)

- [ ] **Step 6: Build + test**

Run: `go build ./... && go test ./pkg/server/config/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/server/config/validate.go pkg/server/config/validate_test.go pkg/server/app.go
git commit -m "feat(server): validate volume paths at startup

Fail fast with a clear message naming the volume and path when a
configured directory is missing or not a directory, instead of erroring
on the first I/O."
```

---

## Task 5: Mount-spec parser

**Files:**
- Create: `cmd/commands/mountspec.go`
- Test: `cmd/commands/mountspec_test.go`

Parses `[user@]host[:port]/volume` into parts. Port defaults to 9449.

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MountSpecSuite struct{ suite.Suite }

func TestMountSpecSuite(t *testing.T) { suite.Run(t, new(MountSpecSuite)) }

func (s *MountSpecSuite) TestParses() {
	cases := []struct {
		in                       string
		user, host, vol          string
		port                     int
	}{
		{"admin@host.example:9449/shared", "admin", "host.example", "shared", 9449},
		{"host.example/shared", "", "host.example", "shared", 9449},
		{"admin@10.0.0.5/data", "admin", "10.0.0.5", "data", 9449},
		{"host:7000/vol", "", "host", "vol", 7000},
	}
	for _, c := range cases {
		got, err := parseMountSpec(c.in)
		s.Require().NoError(err, c.in)
		s.Equal(c.user, got.Username, c.in)
		s.Equal(c.host, got.Host, c.in)
		s.Equal(c.port, got.Port, c.in)
		s.Equal(c.vol, got.Volume, c.in)
	}
}

func (s *MountSpecSuite) TestRejectsMalformed() {
	for _, in := range []string{"hostonly", "host/", "/vol", "host:notaport/vol", ""} {
		_, err := parseMountSpec(in)
		s.Require().Error(err, in)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestMountSpecSuite ./cmd/commands/`
Expected: FAIL — `undefined: parseMountSpec`.

- [ ] **Step 3: Implement the parser**

```go
package commands

import (
	"fmt"
	"strconv"
	"strings"
)

const defaultServerPort = 9449

// mountSpec is the parsed form of [user@]host[:port]/volume.
type mountSpec struct {
	Username string
	Host     string
	Port     int
	Volume   string
}

// parseMountSpec parses the sshfs-style shorthand "[user@]host[:port]/volume".
// Port defaults to 9449. Host and volume are required.
func parseMountSpec(s string) (mountSpec, error) {
	const example = `expected [user@]host[:port]/volume, e.g. admin@host:9449/shared`
	var spec mountSpec

	if at := strings.IndexByte(s, '@'); at >= 0 {
		spec.Username = s[:at]
		s = s[at+1:]
		if spec.Username == "" {
			return spec, fmt.Errorf("empty username before '@'; %s", example)
		}
	}

	slash := strings.IndexByte(s, '/')
	if slash < 0 {
		return spec, fmt.Errorf("missing '/volume'; %s", example)
	}
	hostport := s[:slash]
	spec.Volume = s[slash+1:]
	if spec.Volume == "" {
		return spec, fmt.Errorf("empty volume after '/'; %s", example)
	}

	spec.Port = defaultServerPort
	if colon := strings.IndexByte(hostport, ':'); colon >= 0 {
		spec.Host = hostport[:colon]
		portStr := hostport[colon+1:]
		p, err := strconv.Atoi(portStr)
		if err != nil || p < 1 || p > 65535 {
			return spec, fmt.Errorf("invalid port %q; %s", portStr, example)
		}
		spec.Port = p
	} else {
		spec.Host = hostport
	}
	if spec.Host == "" {
		return spec, fmt.Errorf("missing host; %s", example)
	}
	return spec, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestMountSpecSuite ./cmd/commands/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/commands/mountspec.go cmd/commands/mountspec_test.go
git commit -m "feat(cli): parse sshfs-style mount shorthand

parseMountSpec turns [user@]host[:port]/volume into its parts (port
defaults to 9449); table-tested for valid and malformed inputs."
```

---

## Task 6: Password resolver

**Files:**
- Create: `cmd/commands/credentials.go`
- Test: `cmd/commands/credentials_test.go`

Resolution order: explicit value (flag) → `GMOUNTIE_AUTH_PASSWORD` env → TTY
prompt → error if non-interactive.

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type CredentialsSuite struct{ suite.Suite }

func TestCredentialsSuite(t *testing.T) { suite.Run(t, new(CredentialsSuite)) }

func (s *CredentialsSuite) TestFlagWins() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "fromenv")
	got, err := resolvePassword("fromflag", strings.NewReader(""), &bytes.Buffer{})
	s.Require().NoError(err)
	s.Equal("fromflag", got)
}

func (s *CredentialsSuite) TestEnvUsedWhenNoFlag() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "fromenv")
	got, err := resolvePassword("", strings.NewReader(""), &bytes.Buffer{})
	s.Require().NoError(err)
	s.Equal("fromenv", got)
}

func (s *CredentialsSuite) TestPromptsFromNonFileReader() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "")
	var prompt bytes.Buffer
	got, err := resolvePassword("", strings.NewReader("typed\n"), &prompt)
	s.Require().NoError(err)
	s.Equal("typed", got)
	s.Contains(prompt.String(), "Password")
}

func (s *CredentialsSuite) TestErrorsWhenEmptyAndNoInput() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "")
	_, err := resolvePassword("", strings.NewReader(""), &bytes.Buffer{})
	s.Require().Error(err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestCredentialsSuite ./cmd/commands/`
Expected: FAIL — `undefined: resolvePassword`.

- [ ] **Step 3: Implement the resolver**

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestCredentialsSuite ./cmd/commands/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/commands/credentials.go cmd/commands/credentials_test.go
git commit -m "feat(cli): resolve password from flag, env, or prompt

Add resolvePassword (flag > GMOUNTIE_AUTH_PASSWORD > no-echo TTY prompt >
error) so credentials need not sit on the command line."
```

---

## Task 7: Error remediation classifier

**Files:**
- Create: `cmd/commands/clienterr.go`
- Test: `cmd/commands/clienterr_test.go`

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ClientErrSuite struct{ suite.Suite }

func TestClientErrSuite(t *testing.T) { suite.Run(t, new(ClientErrSuite)) }

func (s *ClientErrSuite) TestUnreachable() {
	err := remediate(errors.New("connection refused"), "host:9449", "shared")
	s.Contains(err.Error(), "unreachable")
	s.Contains(err.Error(), "host:9449")
}

func (s *ClientErrSuite) TestAuthFailed() {
	err := remediate(status.Error(codes.Unauthenticated, "bad creds"), "host:9449", "shared")
	s.Contains(err.Error(), "authentication failed")
}

func (s *ClientErrSuite) TestVolumeNotFound() {
	err := remediate(status.Error(codes.NotFound, "no volume"), "host:9449", "shared")
	s.Contains(err.Error(), "shared")
	s.Contains(err.Error(), "gmountie ls")
}

func (s *ClientErrSuite) TestNilPassesThrough() {
	s.NoError(remediate(nil, "host:9449", "shared"))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestClientErrSuite ./cmd/commands/`
Expected: FAIL — `undefined: remediate`.

- [ ] **Step 3: Implement the classifier**

```go
package commands

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// remediate wraps a client-side error with an actionable hint based on its
// kind. addr is the server endpoint, volume the target volume (may be "").
func remediate(err error, addr, volume string) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return fmt.Errorf("authentication failed connecting to %s — check username/password "+
			"(server stores argon2id hashes; generate with `gmountie genpass`): %w", addr, err)
	case codes.NotFound:
		return fmt.Errorf("volume %q not found on %s — run `gmountie ls %s` to list available volumes: %w",
			volume, addr, addr, err)
	}
	if isUnreachable(err) {
		return fmt.Errorf("server unreachable at %s — check the address/port, firewall, and that "+
			"`gmountie serve` is running: %w", addr, err)
	}
	return err
}

func isUnreachable(err error) bool {
	if status.Code(err) == codes.Unavailable {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, frag := range []string{"connection refused", "no route to host", "i/o timeout", "no such host"} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestClientErrSuite ./cmd/commands/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/commands/clienterr.go cmd/commands/clienterr_test.go
git commit -m "feat(cli): add error remediation hints

remediate() maps connect/auth/not-found failures to actionable messages
(check serve running, check creds, run gmountie ls)."
```

---

## Task 8: Daemon arg-builder + spawn seam

**Files:**
- Create: `cmd/commands/daemon.go`
- Test: `cmd/commands/daemon_test.go`

The pure, tested part is the child-arg construction and the seam interface. The
actual `exec`/`setsid` lives behind the seam (covered by e2e on the VM, not unit
tests).

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type DaemonSuite struct{ suite.Suite }

func TestDaemonSuite(t *testing.T) { suite.Run(t, new(DaemonSuite)) }

func (s *DaemonSuite) TestStripsDaemonFlagAndKeepsRest() {
	args := buildDaemonChildArgs([]string{"mount", "admin@host/shared", "/mnt", "--daemon"})
	s.NotContains(args, "--daemon")
	s.Contains(args, "mount")
	s.Contains(args, "admin@host/shared")
	s.Contains(args, "/mnt")
}

func (s *DaemonSuite) TestStripsDaemonEqualsForm() {
	args := buildDaemonChildArgs([]string{"mount", "/mnt", "--daemon=true"})
	for _, a := range args {
		s.NotContains(a, "--daemon")
	}
}

func (s *DaemonSuite) TestParentWaitsForReadyViaSeam() {
	fake := &fakeDaemonizer{ready: true}
	err := daemonize(fake, []string{"mount", "/mnt", "--daemon"})
	s.Require().NoError(err)
	s.True(fake.spawned)
}

func (s *DaemonSuite) TestParentReportsChildFailure() {
	fake := &fakeDaemonizer{ready: false}
	err := daemonize(fake, []string{"mount", "/mnt", "--daemon"})
	s.Require().Error(err)
}

type fakeDaemonizer struct {
	ready   bool
	spawned bool
}

func (f *fakeDaemonizer) spawnAndAwaitReady(childArgs []string) error {
	f.spawned = true
	if !f.ready {
		return errReady
	}
	return nil
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestDaemonSuite ./cmd/commands/`
Expected: FAIL — undefined `buildDaemonChildArgs`, `daemonize`, `daemonizer`, `errReady`.

- [ ] **Step 3: Implement arg-builder + seam + default spawner**

```go
//go:build linux

package commands

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/adrg/xdg"
)

// daemonChildEnv marks the re-exec'd child so it runs the foreground mount loop
// instead of forking again.
const daemonChildEnv = "GMOUNTIE_DAEMON_CHILD"

// readyFD is the inherited pipe fd (after the standard three) the child writes
// to once the mount is up.
const readyFD = 3

var errReady = errors.New("daemon child exited before signalling mount ready")

// daemonizer is the seam: the parent side of --daemon. Faked in tests.
type daemonizer interface {
	spawnAndAwaitReady(childArgs []string) error
}

// buildDaemonChildArgs returns os.Args-style args for the child with any
// --daemon / --daemon=... flag removed (so the child runs in the foreground).
func buildDaemonChildArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		if a == "--daemon" || strings.HasPrefix(a, "--daemon=") {
			continue
		}
		out = append(out, a)
	}
	return out
}

// daemonize runs the parent side: spawn the child, wait until it signals the
// mount is ready, then return (the caller exits 0). Errors if the child dies
// first.
func daemonize(d daemonizer, fullArgs []string) error {
	return d.spawnAndAwaitReady(buildDaemonChildArgs(fullArgs[1:])) // drop argv[0]
}

// isDaemonChild reports whether this process is the re-exec'd child.
func isDaemonChild() bool { return os.Getenv(daemonChildEnv) == "1" }

// daemonLogPath is where the detached child's stdout/stderr go.
func daemonLogPath() string {
	return filepath.Join(xdg.StateHome, "gmountie", "mount-daemon.log")
}

// execDaemonizer is the production seam: re-execs the current binary detached
// (new session), redirecting output to a log file, and waits for the readyFD
// pipe to report success.
type execDaemonizer struct{}

func (execDaemonizer) spawnAndAwaitReady(childArgs []string) error {
	logPath := daemonLogPath()
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return fmt.Errorf("create daemon log dir: %w", err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open daemon log %s: %w", logPath, err)
	}
	defer logFile.Close()

	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create ready pipe: %w", err)
	}
	defer pr.Close()

	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	cmd := exec.Command(self, childArgs...)
	cmd.Env = append(os.Environ(), daemonChildEnv+"=1")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.ExtraFiles = []*os.File{pw} // becomes fd 3 in the child
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		pw.Close()
		return fmt.Errorf("start daemon child: %w", err)
	}
	pw.Close() // parent keeps only the read end

	buf := make([]byte, 8)
	n, _ := io.ReadFull(pr, buf[:5]) // child writes "ready"
	if n < 5 {
		return fmt.Errorf("%w (see %s)", errReady, logPath)
	}
	fmt.Fprintf(os.Stderr, "gMountie: mounted in background (pid %d, logs: %s)\n", cmd.Process.Pid, logPath)
	return nil
}

// signalDaemonReady is called by the child after a successful mount to release
// the waiting parent. No-op when not a daemon child.
func signalDaemonReady() {
	if !isDaemonChild() {
		return
	}
	f := os.NewFile(readyFD, "ready-pipe")
	if f == nil {
		return
	}
	_, _ = f.WriteString("ready")
	_ = f.Close()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestDaemonSuite ./cmd/commands/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/commands/daemon.go cmd/commands/daemon_test.go
git commit -m "feat(cli): add --daemon re-exec orchestration

buildDaemonChildArgs + daemonizer seam (faked in tests) and an
execDaemonizer that re-execs the binary detached (setsid, log file) and
waits on a ready pipe the child signals after a successful mount."
```

---

## Task 9: Wire shorthand, password resolver, remediation, and --daemon into `mount`

**Files:**
- Modify: `cmd/commands/mount.go`
- Test: `cmd/commands/mount_test.go` (add a shorthand-application test)

- [ ] **Step 1: Write the failing test**

```go
func (s *MountSuite) TestApplyMountSpecPopulatesFlags() {
	// applyMountSpec maps a parsed spec onto the viper instance.
	v := viper.New()
	spec := mountSpec{Username: "admin", Host: "10.0.0.5", Port: 9449, Volume: "shared"}
	applyMountSpec(v, spec)
	s.Equal("10.0.0.5", v.GetString("server.address"))
	s.Equal("9449", v.GetString("server.port"))
	s.Equal("admin", v.GetString("auth.username"))
	s.Equal("shared", v.GetString("auth.type") /* placeholder */ != "" && false || "shared", "volume captured")
}
```

Simplify that last assertion — write it as:

```go
func (s *MountSuite) TestApplyMountSpecPopulatesFlags() {
	v := viper.New()
	spec := mountSpec{Username: "admin", Host: "10.0.0.5", Port: 9449, Volume: "shared"}
	gotVol := applyMountSpec(v, spec)
	s.Equal("10.0.0.5", v.GetString("server.address"))
	s.Equal("9449", v.GetString("server.port"))
	s.Equal("admin", v.GetString("auth.username"))
	s.Equal("shared", gotVol)
}
```

(Add `MountSuite` if absent, mirroring the other suites. Import `github.com/spf13/viper`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestMountSuite ./cmd/commands/`
Expected: FAIL — `undefined: applyMountSpec`.

- [ ] **Step 3: Add `applyMountSpec` and the `--daemon` flag var**

In `mount.go`, add to the `var (...)` block: `daemonFlag bool`. Add helper:

```go
// applyMountSpec maps a parsed shorthand spec onto the viper instance and
// returns the volume name it carried. Explicit flags (checked by the caller)
// still take precedence over these values.
func applyMountSpec(v *viper.Viper, spec mountSpec) string {
	v.Set("server.address", spec.Host)
	v.Set("server.port", fmt.Sprintf("%d", spec.Port))
	if spec.Username != "" {
		v.Set("auth.username", spec.Username)
	}
	return spec.Volume
}
```

- [ ] **Step 4: Run the helper test to verify it passes**

Run: `go test -v -run TestMountSuite ./cmd/commands/`
Expected: PASS.

- [ ] **Step 5: Rewire `mountCmd`**

Replace `Args: cobra.ExactArgs(1)` with `Args: cobra.RangeArgs(1, 2)` and update the `Use`/`Long`:

```go
	Use:   "mount [user@host[:port]/volume] mountpoint",
	Short: "Mount a gMountie volume",
	Long: "Mount a gMountie volume at the given mountpoint.\n\n" +
		"Shorthand:  gmountie mount admin@host:9449/shared /mnt/shared\n" +
		"Or flags:   gmountie mount /mnt/shared -s host:9449 -n shared -u admin\n\n" +
		"The password is taken from --password, then $GMOUNTIE_AUTH_PASSWORD, then\n" +
		"an interactive prompt. Use --daemon to mount in the background.",
	Args: cobra.RangeArgs(1, 2),
```

In `RunE`, restructure the top so the shorthand resolves the mountpoint and seeds
viper before the existing flag-layering. Replace the body from the start of
`RunE` through the `setFromFlag("password", ...)` line with:

```go
	RunE: func(cmd *cobra.Command, args []string) error {
		// Positional forms:
		//   2 args: "<spec> <mountpoint>"  — spec seeds server/user/volume
		//   1 arg : "<mountpoint>"         — flags/config supply the rest
		var mountpoint string
		v := viper.New()
		hasConfig := configFile != ""
		if hasConfig {
			v.SetConfigFile(configFile)
			if err := v.ReadInConfig(); err != nil {
				return fmt.Errorf("failed to read config file %s: %w", configFile, err)
			}
		}

		if len(args) == 2 {
			spec, err := parseMountSpec(args[0])
			if err != nil {
				return err
			}
			if volumeName == "" {
				volumeName = applyMountSpec(v, spec)
			} else {
				applyMountSpec(v, spec) // explicit --volume wins; still seed server/user
			}
			mountpoint = args[1]
		} else {
			mountpoint = args[0]
		}

		// applyServer: -s "host:port" splits into two viper keys (overrides spec
		// only when the user explicitly set --server).
		if cmd.Flags().Changed("server") || (!hasConfig && len(args) < 2) {
			endpointSlice := strings.Split(serverAddr, ":")
			if len(endpointSlice) != 2 {
				return fmt.Errorf("invalid server address: %s", serverAddr)
			}
			v.Set("server.address", endpointSlice[0])
			v.Set("server.port", endpointSlice[1])
		}
		setFromFlag := func(name, viperKey, value string) {
			if cmd.Flags().Changed(name) || (!hasConfig && v.GetString(viperKey) == "") {
				v.Set(viperKey, value)
			}
		}
		setFromFlag("auth-type", "auth.type", authType)
		setFromFlag("username", "auth.username", username)

		if volumeName == "" {
			return fmt.Errorf("volume name is required (use the shorthand host/volume or -n)")
		}

		// Resolve the password without leaving it on the command line.
		if v.GetString("auth.type") == "basic" || (!hasConfig && authType == "basic") {
			if v.GetString("auth.username") == "" {
				return fmt.Errorf("username is required for basic auth (use user@host or -u)")
			}
			pw, err := resolvePassword(password, cmd.InOrStdin(), cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			v.Set("auth.password", pw)
		}
```

Then keep the existing `config.ParseConfig(v)` → mounter flow, with two changes:

1. Replace the `cobra.ExactArgs`-era mountpoint stat/`args[0]` usage further down — it already uses a `mountpoint` var; ensure the later `mountpoint := args[0]` line is **removed** (now set above).
2. Daemon handoff: immediately after the mountpoint-exists check and before
   `grpc.NewClientFromConfig`, add:

```go
		if daemonFlag {
			return daemonize(execDaemonizer{}, os.Args)
		}
```

3. Wrap client/mount errors with `remediate`. Change:
   - `return fmt.Errorf("failed to create client: %w", err)` →
     `return remediate(err, net.JoinHostPort(cfg.Server.Address, fmt.Sprintf("%d", cfg.Server.Port)), volumeName)`
   - the `mounter.Mount` error similarly wrapped with `remediate`.
   Add `"net"` to imports.

4. After the successful `mounter.Mount` + log lines, signal a waiting parent:

```go
		signalDaemonReady()
```

(placed right before the `Press Ctrl+C` log / signal wait).

Update `init()` to register the flag:

```go
	mountCmd.PersistentFlags().BoolVar(&daemonFlag, "daemon", false, "mount in the background (detach after the mount is ready)")
```

Also update the `--password` flag help: `"password for basic auth (visible in ps/history; prefer the prompt or $GMOUNTIE_AUTH_PASSWORD)"`.

- [ ] **Step 6: Build + full command tests**

Run: `go build ./... && go test ./cmd/commands/...`
Expected: PASS. (Real FUSE mount is not exercised here; the daemon path is gated by the seam and only reached with `--daemon`.)

- [ ] **Step 7: Manual sanity (no mount)**

Run: `go run . mount --help`
Expected: usage shows the shorthand and `--daemon`.

- [ ] **Step 8: Commit**

```bash
git add cmd/commands/mount.go cmd/commands/mount_test.go
git commit -m "feat(mount): shorthand, prompt-based password, --daemon, better errors

Accept [user@]host[:port]/volume mountpoint, resolve the password via
flag/env/prompt instead of requiring -p, add --daemon background mode,
and wrap connect/auth/volume failures with remediation hints."
```

---

## Task 10: systemd units

**Files:**
- Create: `packaging/systemd/gmountie-serve.service`
- Create: `packaging/systemd/gmountie-mount@.service`
- Create: `packaging/systemd/gmountie.env.example`

- [ ] **Step 1: Write `gmountie-serve.service`**

```ini
[Unit]
Description=gMountie server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/gmountie serve
Restart=on-failure
# Config defaults to ~/.config/gmountie/server.yaml; set GMOUNTIE_* overrides here.
Environment=XDG_CONFIG_HOME=%h/.config

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Write `gmountie-mount@.service`**

The instance name encodes the volume; mountpoint + server come from the env file.

```ini
[Unit]
Description=gMountie mount of volume %i
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/gmountie/%i.env
# %i is the volume name; SERVER and MOUNTPOINT come from the env file.
ExecStart=/usr/local/bin/gmountie mount ${SERVER}/%i ${MOUNTPOINT}
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 3: Write `gmountie.env.example`**

```sh
# Copy to /etc/gmountie/<volume>.env (e.g. /etc/gmountie/shared.env) and edit.
# Used by gmountie-mount@<volume>.service.
SERVER=admin@host.example:9449
MOUNTPOINT=/mnt/shared
GMOUNTIE_AUTH_PASSWORD=replace-me
```

- [ ] **Step 4: Verify the unit files are well-formed (no install needed)**

Run: `systemd-analyze verify packaging/systemd/gmountie-serve.service || true`
Expected: no fatal parse errors (warnings about absolute paths/install in a
non-system context are acceptable). If `systemd-analyze` is unavailable, skip.

- [ ] **Step 5: Commit**

```bash
git add packaging/systemd/
git commit -m "feat(packaging): add systemd units for serve and mount

gmountie-serve.service plus a templated gmountie-mount@.service that reads
SERVER/MOUNTPOINT/password from /etc/gmountie/<volume>.env."
```

---

## Task 11: `gmountie ls`

**Files:**
- Create: `cmd/commands/ls.go`
- Test: `cmd/commands/ls_test.go`

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/suite"
	"gmountie/pkg/proto"
)

type LsSuite struct{ suite.Suite }

func TestLsSuite(t *testing.T) { suite.Run(t, new(LsSuite)) }

func (s *LsSuite) TestRenderVolumes() {
	var out bytes.Buffer
	renderVolumes(&out, []*proto.Volume{{Name: "shared"}, {Name: "backups"}})
	s.Contains(out.String(), "shared")
	s.Contains(out.String(), "backups")
}

func (s *LsSuite) TestRenderEmpty() {
	var out bytes.Buffer
	renderVolumes(&out, nil)
	s.Contains(out.String(), "no volumes")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestLsSuite ./cmd/commands/`
Expected: FAIL — `undefined: renderVolumes`.

- [ ] **Step 3: Implement the command**

```go
package commands

import (
	"context"
	"fmt"
	"io"
	"net"

	"gmountie/pkg/client/config"
	"gmountie/pkg/client/grpc"
	"gmountie/pkg/proto"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var lsCmd = &cobra.Command{
	Use:   "ls [user@host[:port]]",
	Short: "List the volumes a gMountie server exposes",
	Long: "Connect to a server and list its available volumes.\n\n" +
		"  gmountie ls admin@host:9449\n" +
		"  gmountie ls -c client.yaml",
	Args: cobra.MaximumNArgs(1),
	RunE: runLs,
}

func init() {
	lsCmd.PersistentFlags().StringVarP(&authType, "auth-type", "t", "basic", "authentication type (basic)")
	lsCmd.PersistentFlags().StringVarP(&username, "username", "u", "", "username for basic auth")
	lsCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "password for basic auth (prefer prompt/$GMOUNTIE_AUTH_PASSWORD)")
	rootCmd.AddCommand(lsCmd)
}

func runLs(cmd *cobra.Command, args []string) error {
	v := viper.New()
	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read config file %s: %w", configFile, err)
		}
	}
	if len(args) == 1 {
		// Reuse the spec parser for the host portion by appending a sentinel
		// volume, which ls does not need.
		spec, err := parseMountSpec(args[0] + "/_")
		if err != nil {
			return err
		}
		applyMountSpec(v, spec)
	}
	if cmd.Flags().Changed("username") {
		v.Set("auth.username", username)
	}
	if cmd.Flags().Changed("auth-type") || v.GetString("auth.type") == "" {
		v.Set("auth.type", authType)
	}
	if v.GetString("auth.type") == "basic" {
		if v.GetString("auth.username") == "" {
			return fmt.Errorf("username is required for basic auth (use user@host or -u)")
		}
		pw, err := resolvePassword(password, cmd.InOrStdin(), cmd.ErrOrStderr())
		if err != nil {
			return err
		}
		v.Set("auth.password", pw)
	}

	cfg, err := config.ParseConfig(v)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	addr := net.JoinHostPort(cfg.Server.Address, fmt.Sprintf("%d", cfg.Server.Port))

	c, err := grpc.NewClientFromConfig(cfg)
	if err != nil {
		return remediate(err, addr, "")
	}
	defer c.Close()

	reply, err := c.Volume().List(context.Background(), &proto.VolumeListRequest{})
	if err != nil {
		return remediate(err, addr, "")
	}
	renderVolumes(cmd.OutOrStdout(), reply.GetVolumes())
	return nil
}

// renderVolumes prints one volume name per line, or a friendly note if empty.
func renderVolumes(out io.Writer, vols []*proto.Volume) {
	if len(vols) == 0 {
		fmt.Fprintln(out, "no volumes available")
		return
	}
	for _, vol := range vols {
		fmt.Fprintln(out, vol.GetName())
	}
}
```

> Note: `parseMountSpec(args[0] + "/_")` requires a volume segment; the `_`
> sentinel is discarded. If `ls` is given a bare `host`, the user still gets a
> clear parse error pointing at the shorthand — acceptable.

- [ ] **Step 4: Run test + build**

Run: `go test -v -run TestLsSuite ./cmd/commands/ && go build ./...`
Expected: PASS, clean build.

- [ ] **Step 5: Commit**

```bash
git add cmd/commands/ls.go cmd/commands/ls_test.go
git commit -m "feat(cli): add gmountie ls to list server volumes

Reuses VolumeService.List and the shared spec/password resolution so users
can discover available volumes before mounting."
```

---

## Task 12: `gmountie config show`

**Files:**
- Create: `cmd/commands/configshow.go`
- Test: `cmd/commands/configshow_test.go`

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ConfigShowSuite struct{ suite.Suite }

func TestConfigShowSuite(t *testing.T) { suite.Run(t, new(ConfigShowSuite)) }

func (s *ConfigShowSuite) TestRedactsSecrets() {
	in := "auth:\n  username: admin\n  password: supersecret\nserver:\n  address: 1.2.3.4\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "supersecret")
	s.Contains(out, "REDACTED")
	s.Contains(out, "admin")
	s.Contains(out, "1.2.3.4")
}

func (s *ConfigShowSuite) TestRedactsPasswordHash() {
	in := "auth:\n  users:\n    - username: admin\n      password_hash: $argon2id$abc\n"
	out := redactConfigYAML(in)
	s.NotContains(out, "argon2id$abc")
	s.Contains(out, "REDACTED")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestConfigShowSuite ./cmd/commands/`
Expected: FAIL — `undefined: redactConfigYAML`.

- [ ] **Step 3: Implement redaction + command**

```go
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
	fmt.Fprintf(cmd.OutOrStdout(), "# %s\n%s\n", path, redactConfigYAML(string(data)))
	return nil
}
```

- [ ] **Step 4: Run test + build**

Run: `go test -v -run TestConfigShowSuite ./cmd/commands/ && go build ./...`
Expected: PASS, clean build.

- [ ] **Step 5: Commit**

```bash
git add cmd/commands/configshow.go cmd/commands/configshow_test.go
git commit -m "feat(cli): add gmountie config show

Print the config file gMountie would load with passwords/hashes redacted,
making file/flag/env precedence debuggable."
```

---

## Task 13: Paste-ready fingerprint snippet

**Files:**
- Modify: `cmd/commands/fingerprint.go:52-56`
- Test: `cmd/commands/fingerprint_test.go` (add a method)

- [ ] **Step 1: Write the failing test**

```go
func (s *FingerprintSuite) TestNonVerbosePrintsPasteSnippet() {
	out := renderFingerprint("SHA256:abc123")
	s.Contains(out, "SHA256:abc123")
	s.Contains(out, "expected_fingerprint: SHA256:abc123")
}
```

(Add a `FingerprintSuite` if none exists.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestFingerprintSuite ./cmd/commands/`
Expected: FAIL — `undefined: renderFingerprint`.

- [ ] **Step 3: Implement `renderFingerprint` and use it**

Add to `fingerprint.go`:

```go
// renderFingerprint formats the one-line (non-verbose) output: the raw
// fingerprint plus a copy-paste-ready client config snippet.
func renderFingerprint(fp string) string {
	return fmt.Sprintf("%s\n\n# Add to client.yaml under server.tls:\n#   verify: true\n#   expected_fingerprint: %s\n", fp, fp)
}
```

Replace the non-verbose branch (lines 53-56):

```go
	if !fingerprintVerbose {
		_, _ = fmt.Fprint(out, renderFingerprint(fp))
		return nil
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestFingerprintSuite ./cmd/commands/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/commands/fingerprint.go cmd/commands/fingerprint_test.go
git commit -m "feat(fingerprint): emit paste-ready client snippet

Print the expected_fingerprint: client.yaml block alongside the raw
fingerprint so TOFU pinning is copy-paste."
```

---

## Task 14: Richer global flag help

**Files:**
- Modify: `cmd/commands/root.go:26-29`

- [ ] **Step 1: Update the flag descriptions**

```go
func init() {
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "",
		"path to a server.yaml or client.yaml; explicit flags override file values, which override $GMOUNTIE_* env vars")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false,
		"enable verbose (debug-level) logging")
}
```

- [ ] **Step 2: Build + verify help text**

Run: `go build ./... && go run . --help`
Expected: clean build; `--config` help shows the precedence note.

- [ ] **Step 3: Commit**

```bash
git add cmd/commands/root.go
git commit -m "docs(cli): clarify --config/--verbose help

State accepted config files and flag>file>env precedence in the --config
help text."
```

---

## Task 15: Documentation + release notes

**Files:**
- Modify: `README.md` (quickstart section)
- Modify: `website/docs/quickstart.*` (the Docusaurus quickstart; find exact file with `ls website/docs`)
- Modify: `docs/design/security-and-transport.md` (note random first-run password + TOFU pinning UX)
- Create: `docs/RELEASE_NOTES_cli-ux.md`

- [ ] **Step 1: Rewrite the quickstart happy path**

Update README and the Docusaurus quickstart to the new flow. Server:

````markdown
## Quickstart

### Server
```bash
gmountie serve
# First run prints a generated admin password — save it. A "shared" volume
# is exposed at $XDG_DATA_HOME/gmountie/shared and the server binds 0.0.0.0.
# Edit ~/.config/gmountie/server.yaml to add volumes or restrict the address.
```

### Client
```bash
mkdir -p /mnt/shared
# Discover what's available:
gmountie ls admin@SERVER:9449
# Mount (you'll be prompted for the password, or set $GMOUNTIE_AUTH_PASSWORD):
gmountie mount admin@SERVER:9449/shared /mnt/shared
# Background instead of blocking:
gmountie mount admin@SERVER:9449/shared /mnt/shared --daemon
```

Passwords in `auth.users[].password_hash` are argon2id hashes — generate one
with `gmountie genpass`. For TLS pinning, run `gmountie fingerprint` on the
server and paste the printed `expected_fingerprint:` block into `client.yaml`.
````

Remove any prior text claiming the password is `admin` or that the server binds
localhost / that TLS "isn't shipped".

- [ ] **Step 2: Add a security/transport note**

In `docs/design/security-and-transport.md`, add a short subsection noting: first
run generates a random admin password (printed once, argon2id-hashed on disk);
default bind is `0.0.0.0` justified by random password + auto-generated TLS; TOFU
pinning is via `gmountie fingerprint` → `expected_fingerprint`.

- [ ] **Step 3: Write release notes (behavior breaks)**

```markdown
# CLI/Config UX overhaul — release notes

**Behavior changes (no backwards-compatibility shims):**

- `gmountie serve` first run now **binds 0.0.0.0** (was 127.0.0.1), ships a
  working **`shared`** volume at `$XDG_DATA_HOME/gmountie/shared`, and generates
  a **random admin password printed once** (was the fixed `admin`). Rotate with
  `gmountie genpass`.
- The server now **validates volume paths at startup** and refuses to start if a
  configured path is missing or not a directory (was: failed lazily at first I/O).
- `gmountie mount` accepts the shorthand **`[user@]host[:port]/volume mountpoint`**
  and resolves the password from `--password`, then `$GMOUNTIE_AUTH_PASSWORD`,
  then an interactive prompt. `--daemon` mounts in the background.

**New commands:** `gmountie ls`, `gmountie config show`.
```

- [ ] **Step 4: Commit**

```bash
git add README.md website/ docs/design/security-and-transport.md docs/RELEASE_NOTES_cli-ux.md
git commit -m "docs: rewrite quickstart for the new CLI UX

Document the zero-edit first run, ls/mount shorthand, prompt-based
password, --daemon, and TOFU pinning; record behavior breaks in release
notes."
```

---

## Task 16: Final verification

- [ ] **Step 1: Full test suite**

Run: `go test -failfast ./...`
Expected: PASS. (FUSE-mount e2e suites that require `/dev/fuse` are env-gated and
will be exercised in CI / on the VM, not here.)

- [ ] **Step 2: Lint**

Run: `task lint`
Expected: no new findings.

- [ ] **Step 3: Build**

Run: `task build`
Expected: snapshot build succeeds.

- [ ] **Step 4: Smoke the help surface**

Run: `go run . --help && go run . mount --help && go run . ls --help && go run . config show --help`
Expected: each shows the updated usage.

- [ ] **Step 5: VM mount validation (manual, per project convention)**

On the kubevirt VM (or a real terminal with `/dev/fuse`): start `gmountie serve`,
confirm the printed password + `shared` volume, then from the client run
`gmountie ls`, a foreground `gmountie mount admin@host/shared /mnt/shared`, and a
`--daemon` mount; verify files are visible and `--daemon` returns after the mount
is ready (check `mount-daemon.log`). Record the result in the PR description.

- [ ] **Step 6: Open the PR**

Push the branch and open a PR summarizing the UX overhaul, linking this plan and
the design spec, and pasting the release-notes behavior-break list.

---

## Self-Review

**Spec coverage:**
- §1 first-run server → Tasks 2, 3 (random pw, default volume, 0.0.0.0, dir create). ✓
- §1d startup path validation → Task 4. ✓
- §2a shorthand → Tasks 5, 9. ✓
- §2b password resolution → Tasks 1, 6, 9. ✓
- §2c daemon → Tasks 8, 9. §2d systemd → Task 10. ✓
- §2e error remediation → Tasks 7, 9. ✓
- §3a `ls` → Task 11. §3b `config show` → Task 12. ✓
- §4 help/fingerprint/docs → Tasks 13, 14, 15. ✓
- Testing/delivery conventions → Task 16 + per-task commits. ✓

**Placeholder scan:** No TBD/TODO. The one weak test assertion draft in Task 9
Step 1 is explicitly replaced with the clean version in the same step.

**Type consistency:** `mountSpec{Username,Host,Port,Volume}`, `parseMountSpec`,
`applyMountSpec`, `resolvePassword(flag,in,prompt)`, `remediate(err,addr,volume)`,
`buildDaemonChildArgs`/`daemonize`/`daemonizer`/`signalDaemonReady`,
`renderVolumes`, `redactConfigYAML`, `renderFingerprint`,
`buildFirstRunConfig`/`defaultVolumeDir`, `(*Config).ValidateVolumePaths` — names
are used consistently across the tasks that reference them.

**Known follow-up risks (flagged, not blockers):**
- The `mount.go` rewrite (Task 9 Step 5) is the largest single edit; the executor
  should diff carefully against the current precedence logic and keep the
  `rawIDs`/`cfg.Mount` handling that follows `ParseConfig` intact.
- `ls`'s reuse of `parseMountSpec` via a `/_` sentinel is a deliberate shortcut;
  if it reads awkwardly, factor a `parseHostPort` helper instead.
