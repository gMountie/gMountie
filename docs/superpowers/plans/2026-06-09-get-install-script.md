# `get.gmountie.dev` Install Script Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a POSIX `install.sh` that installs the `gmountie` CLI from GitHub Releases, served at `https://docs.gmountie.dev/install.sh` and fronted by a Cloudflare redirect so `curl -fsSL https://get.gmountie.dev | sh` works.

**Architecture:** A single POSIX `sh` script whose logic is split into small, pure functions (platform detection, version resolution, artifact selection from `checksums.txt`, verification, install-dir selection) with all network/system IO behind thin overridable wrappers. Pure functions are unit-tested with a dependency-free POSIX test harness; the whole script is shellcheck-clean; a CI smoke job runs it end-to-end against real releases. Hosting is a static asset published by the existing docs Pages build, plus a Cloudflare redirect rule (an ops step, documented, not code).

**Tech Stack:** POSIX `sh`, `shellcheck`, GitHub Actions, GitHub Releases (goreleaser artifacts), Cloudflare.

---

## Key facts (verified, do not re-derive)

- GitHub repo: `gMountie/gMountie` (URLs are case-insensitive).
- goreleaser archive `name_template` is `{{.ProjectName}}_{{.Version}}_{{.Os}}_{{.Arch}}` and `.Version` **strips the leading `v`** — so the script must **never reconstruct** the filename. It reads it from `checksums.txt`.
- goreleaser OS/arch matrix: `linux`/`darwin` × `amd64`/`arm64`. Archives are `.tar.gz`; checksums file is `checksums.txt` (sha256); keyless cosign signs `checksums.txt`.
- **No stable release exists** — all tags are `-alpha` prereleases, so `…/releases/latest` redirects to the list page, not a tag. Version resolution prefers stable, then falls back to the API's newest release.
- Canonical script path is `website/static/install.sh` (Docusaurus publishes `website/static/` verbatim at the site root). Tests must **not** live under `website/static/` (they would be published) — they live under `scripts/install/`.
- The docs Pages deploy (`.github/workflows/docs.yml`) runs on `release: published` or manual `workflow_dispatch`.

## File structure

- Create: `website/static/install.sh` — the published install script (canonical source).
- Create: `scripts/install/install_test.sh` — dependency-free POSIX unit tests; sources `install.sh` with `INSTALL_SH_SOURCED=1`.
- Create: `scripts/install/testdata/checksums.txt` — a realistic fixture for artifact-selection / sha tests.
- Modify: `.github/workflows/ci.yml` — add an `install-script` job (shellcheck + unit tests) and a `install-smoke` job (end-to-end against the live release).

## Script conventions (apply to every code step)

- First line `#!/bin/sh`; second line `set -eu`.
- Constants near the top: `REPO="gMountie/gMountie"`, `BIN_NAME="gmountie"`, `GH="https://github.com"`, `API="https://api.github.com"`.
- All control-flow "failure" returns are consumed by an explicit `if`/`||` (never a bare call under `set -e`).
- Sourcing guard at the very bottom so tests can source without executing:

  ```sh
  if [ "${INSTALL_SH_SOURCED:-0}" != "1" ]; then
    main "$@"
  fi
  ```

---

### Task 1: Test harness + script skeleton + `normalize_os`

**Files:**
- Create: `website/static/install.sh`
- Create: `scripts/install/install_test.sh`

- [ ] **Step 1: Create the test harness with the first failing test**

Create `scripts/install/install_test.sh`:

```sh
#!/bin/sh
# Dependency-free POSIX unit tests for website/static/install.sh.
# Sources the script (guarded by INSTALL_SH_SOURCED) and exercises its
# pure functions — no network, no system mutation. Run: sh scripts/install/install_test.sh
set -u

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
INSTALL_SH_SOURCED=1 . "$here/../../website/static/install.sh"

fails=0
assert_eq() { # desc expected actual
  if [ "$2" = "$3" ]; then
    printf 'ok   - %s\n' "$1"
  else
    printf 'FAIL - %s\n      expected: [%s]\n      actual:   [%s]\n' "$1" "$2" "$3"
    fails=$((fails + 1))
  fi
}
assert_fails() { # desc cmd...
  d=$1; shift
  if "$@" >/dev/null 2>&1; then
    printf 'FAIL - %s (expected non-zero exit)\n' "$d"; fails=$((fails + 1))
  else
    printf 'ok   - %s\n' "$d"
  fi
}

# ---- TESTS ----

assert_eq "normalize_os linux"  "linux"  "$(normalize_os Linux)"
assert_eq "normalize_os darwin" "darwin" "$(normalize_os Darwin)"
assert_fails "normalize_os rejects windows" normalize_os MINGW64_NT

# ---- END TESTS ----

if [ "$fails" -gt 0 ]; then
  printf '\n%d test(s) failed\n' "$fails"; exit 1
fi
printf '\nall tests passed\n'
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `sh scripts/install/install_test.sh`
Expected: FAIL — `install.sh` does not exist yet (source error / `normalize_os: not found`).

- [ ] **Step 3: Create `install.sh` skeleton with `normalize_os`**

Create `website/static/install.sh`:

```sh
#!/bin/sh
# gMountie CLI installer. Usage: curl -fsSL https://get.gmountie.dev | sh
# Env: GMOUNTIE_VERSION (pin a tag), BIN_DIR (force install dir).
set -eu

REPO="gMountie/gMountie"
BIN_NAME="gmountie"
GH="https://github.com"
API="https://api.github.com"

die() { printf 'install: %s\n' "$1" >&2; exit 1; }

# normalize_os <uname -s output> -> linux|darwin (exit 1 if unsupported)
normalize_os() {
  case "$1" in
    Linux) printf 'linux' ;;
    Darwin) printf 'darwin' ;;
    *) return 1 ;;
  esac
}

main() {
  die "main not implemented yet"
}

if [ "${INSTALL_SH_SOURCED:-0}" != "1" ]; then
  main "$@"
fi
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `sh scripts/install/install_test.sh`
Expected: PASS — `all tests passed` (3 ok lines).

- [ ] **Step 5: Commit**

```bash
git add website/static/install.sh scripts/install/install_test.sh
git commit -m "feat(install): script skeleton + os detection with test harness"
```

---

### Task 2: `normalize_arch`

**Files:**
- Modify: `website/static/install.sh`
- Modify: `scripts/install/install_test.sh`

- [ ] **Step 1: Add failing tests**

In `scripts/install/install_test.sh`, add before `# ---- END TESTS ----`:

```sh
assert_eq "normalize_arch amd64 (x86_64)"  "amd64" "$(normalize_arch x86_64)"
assert_eq "normalize_arch amd64 (amd64)"   "amd64" "$(normalize_arch amd64)"
assert_eq "normalize_arch arm64 (aarch64)" "arm64" "$(normalize_arch aarch64)"
assert_eq "normalize_arch arm64 (arm64)"   "arm64" "$(normalize_arch arm64)"
assert_fails "normalize_arch rejects i386" normalize_arch i386
```

- [ ] **Step 2: Run to verify it fails**

Run: `sh scripts/install/install_test.sh`
Expected: FAIL — `normalize_arch: not found`.

- [ ] **Step 3: Implement `normalize_arch`**

In `website/static/install.sh`, add after `normalize_os`:

```sh
# normalize_arch <uname -m output> -> amd64|arm64 (exit 1 if unsupported)
normalize_arch() {
  case "$1" in
    x86_64 | amd64) printf 'amd64' ;;
    aarch64 | arm64) printf 'arm64' ;;
    *) return 1 ;;
  esac
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `sh scripts/install/install_test.sh`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add website/static/install.sh scripts/install/install_test.sh
git commit -m "feat(install): arch detection"
```

---

### Task 3: `pick_archive` + fixture

**Files:**
- Modify: `website/static/install.sh`
- Modify: `scripts/install/install_test.sh`
- Create: `scripts/install/testdata/checksums.txt`

- [ ] **Step 1: Create the checksums fixture**

Create `scripts/install/testdata/checksums.txt` (sha256 sums are dummy but well-formed; format matches goreleaser output — `<sha256>␠␠<filename>`):

```
1111111111111111111111111111111111111111111111111111111111111111  gmountie_0.15.0-alpha.0_linux_amd64.tar.gz
2222222222222222222222222222222222222222222222222222222222222222  gmountie_0.15.0-alpha.0_linux_arm64.tar.gz
3333333333333333333333333333333333333333333333333333333333333333  gmountie_0.15.0-alpha.0_darwin_amd64.tar.gz
4444444444444444444444444444444444444444444444444444444444444444  gmountie_0.15.0-alpha.0_darwin_arm64.tar.gz
```

- [ ] **Step 2: Add failing tests**

In `scripts/install/install_test.sh`, add after the source line (near the top, after `INSTALL_SH_SOURCED=1 . ...`):

```sh
checksums=$(cat "$here/testdata/checksums.txt")
```

Then add before `# ---- END TESTS ----`:

```sh
assert_eq "pick_archive linux/amd64" \
  "gmountie_0.15.0-alpha.0_linux_amd64.tar.gz" \
  "$(pick_archive "$checksums" linux amd64)"
assert_eq "pick_archive darwin/arm64" \
  "gmountie_0.15.0-alpha.0_darwin_arm64.tar.gz" \
  "$(pick_archive "$checksums" darwin arm64)"
assert_fails "pick_archive unknown platform" pick_archive "$checksums" plan9 mips
```

- [ ] **Step 3: Run to verify it fails**

Run: `sh scripts/install/install_test.sh`
Expected: FAIL — `pick_archive: not found`.

- [ ] **Step 4: Implement `pick_archive`**

In `website/static/install.sh`, add after `normalize_arch`:

```sh
# pick_archive <checksums.txt contents> <os> <arch> -> archive filename.
# Reads the real artifact name from checksums.txt rather than reconstructing
# it from the goreleaser name template. exit 1 if no matching line.
pick_archive() {
  _name=$(printf '%s\n' "$1" \
    | grep -E "_$2_$3\.tar\.gz\$" \
    | awk '{print $NF}' \
    | head -n1)
  [ -n "$_name" ] || return 1
  printf '%s' "$_name"
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `sh scripts/install/install_test.sh`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add website/static/install.sh scripts/install/install_test.sh scripts/install/testdata/checksums.txt
git commit -m "feat(install): select archive name from checksums.txt"
```

---

### Task 4: `sha_for`

**Files:**
- Modify: `website/static/install.sh`
- Modify: `scripts/install/install_test.sh`

- [ ] **Step 1: Add failing tests**

In `scripts/install/install_test.sh`, add before `# ---- END TESTS ----`:

```sh
assert_eq "sha_for linux/amd64" \
  "1111111111111111111111111111111111111111111111111111111111111111" \
  "$(sha_for "$checksums" gmountie_0.15.0-alpha.0_linux_amd64.tar.gz)"
assert_fails "sha_for missing file" sha_for "$checksums" nope.tar.gz
```

- [ ] **Step 2: Run to verify it fails**

Run: `sh scripts/install/install_test.sh`
Expected: FAIL — `sha_for: not found`.

- [ ] **Step 3: Implement `sha_for`**

In `website/static/install.sh`, add after `pick_archive`:

```sh
# sha_for <checksums.txt contents> <filename> -> expected sha256 hex. exit 1 if absent.
sha_for() {
  _sha=$(printf '%s\n' "$1" | awk -v f="$2" '$NF == f {print $1}' | head -n1)
  [ -n "$_sha" ] || return 1
  printf '%s' "$_sha"
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `sh scripts/install/install_test.sh`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add website/static/install.sh scripts/install/install_test.sh
git commit -m "feat(install): look up expected sha256 from checksums.txt"
```

---

### Task 5: `tag_from_latest_url`

**Files:**
- Modify: `website/static/install.sh`
- Modify: `scripts/install/install_test.sh`

- [ ] **Step 1: Add failing tests**

In `scripts/install/install_test.sh`, add before `# ---- END TESTS ----`:

```sh
assert_eq "tag_from_latest_url stable tag" "v1.2.0" \
  "$(tag_from_latest_url https://github.com/gMountie/gMountie/releases/tag/v1.2.0)"
# When no stable release exists the redirect lands on the list page -> no tag.
assert_fails "tag_from_latest_url list page" \
  tag_from_latest_url https://github.com/gMountie/gMountie/releases
```

- [ ] **Step 2: Run to verify it fails**

Run: `sh scripts/install/install_test.sh`
Expected: FAIL — `tag_from_latest_url: not found`.

- [ ] **Step 3: Implement `tag_from_latest_url`**

In `website/static/install.sh`, add after `sha_for`:

```sh
# tag_from_latest_url <effective url of /releases/latest> -> tag.
# A real stable release redirects to .../releases/tag/<tag>; with no stable
# release GitHub serves .../releases (no /tag/), so we report failure.
tag_from_latest_url() {
  case "$1" in
    */releases/tag/*) printf '%s' "${1##*/releases/tag/}" ;;
    *) return 1 ;;
  esac
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `sh scripts/install/install_test.sh`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add website/static/install.sh scripts/install/install_test.sh
git commit -m "feat(install): parse stable tag from releases/latest redirect"
```

---

### Task 6: `tag_from_releases_json`

**Files:**
- Modify: `website/static/install.sh`
- Modify: `scripts/install/install_test.sh`

- [ ] **Step 1: Add failing tests**

In `scripts/install/install_test.sh`, add before `# ---- END TESTS ----`:

```sh
# Minimal shape of GET /repos/.../releases (newest first), no jq available.
releases_json='[{"tag_name":"v0.15.0-alpha.0","name":"x"},{"tag_name":"v0.14.0-alpha.0"}]'
assert_eq "tag_from_releases_json newest" "v0.15.0-alpha.0" \
  "$(tag_from_releases_json "$releases_json")"
assert_fails "tag_from_releases_json empty" tag_from_releases_json '[]'
```

- [ ] **Step 2: Run to verify it fails**

Run: `sh scripts/install/install_test.sh`
Expected: FAIL — `tag_from_releases_json: not found`.

- [ ] **Step 3: Implement `tag_from_releases_json`**

In `website/static/install.sh`, add after `tag_from_latest_url`:

```sh
# tag_from_releases_json <body of GET /repos/<repo>/releases> -> first tag_name.
# The API returns releases newest-first (including prereleases); we take the
# first "tag_name". Avoids a jq dependency. exit 1 if none present.
tag_from_releases_json() {
  _tag=$(printf '%s' "$1" \
    | tr ',' '\n' \
    | grep -m1 '"tag_name"' \
    | sed -e 's/.*"tag_name"[[:space:]]*:[[:space:]]*"//' -e 's/".*//')
  [ -n "$_tag" ] || return 1
  printf '%s' "$_tag"
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `sh scripts/install/install_test.sh`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add website/static/install.sh scripts/install/install_test.sh
git commit -m "feat(install): parse newest tag from releases API json"
```

---

### Task 7: IO wrappers (`have`, `http_body`, `http_effective_url`, `sha256_of`)

These wrap network/system calls and are intentionally thin (shellcheck-covered, exercised by the smoke job). They exist so higher-level functions can be unit-tested by overriding them.

**Files:**
- Modify: `website/static/install.sh`

- [ ] **Step 1: Implement the IO wrappers**

In `website/static/install.sh`, add after `tag_from_releases_json`:

```sh
# have <cmd> -> 0 if on PATH.
have() { command -v "$1" >/dev/null 2>&1; }

# http_body <url> -> response body on stdout (follows redirects). exit 1 on error.
http_body() {
  if have curl; then
    curl -fsSL "$1"
  elif have wget; then
    wget -qO- "$1"
  else
    die "need curl or wget"
  fi
}

# http_effective_url <url> -> final URL after redirects (HEAD). Requires curl;
# falls back to the input URL when only wget is present (callers tolerate this).
http_effective_url() {
  if have curl; then
    curl -fsSLI -o /dev/null -w '%{url_effective}' "$1"
  else
    printf '%s' "$1"
  fi
}

# sha256_of <file> -> sha256 hex (Linux sha256sum or macOS shasum -a 256).
sha256_of() {
  if have sha256sum; then
    sha256sum "$1" | awk '{print $1}'
  elif have shasum; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    die "need sha256sum or shasum"
  fi
}
```

- [ ] **Step 2: Verify the unit suite still passes (no behavior change to tested fns)**

Run: `sh scripts/install/install_test.sh`
Expected: PASS (unchanged).

- [ ] **Step 3: Commit**

```bash
git add website/static/install.sh
git commit -m "feat(install): http + sha256 io wrappers"
```

---

### Task 8: `resolve_version` (stable-then-prerelease, with IO injected)

**Files:**
- Modify: `website/static/install.sh`
- Modify: `scripts/install/install_test.sh`

- [ ] **Step 1: Add failing tests (override IO wrappers to stub network)**

In `scripts/install/install_test.sh`, add before `# ---- END TESTS ----`:

```sh
# Case A: a stable release exists -> latest redirect yields a tag, API not used.
http_effective_url() { printf '%s' "https://github.com/gMountie/gMountie/releases/tag/v1.0.0"; }
http_body() { printf '%s' "SHOULD_NOT_BE_CALLED"; }
assert_eq "resolve_version prefers stable" "v1.0.0" "$(resolve_version)"

# Case B: no stable -> latest redirect is the list page -> fall back to API newest.
http_effective_url() { printf '%s' "https://github.com/gMountie/gMountie/releases"; }
http_body() { printf '%s' '[{"tag_name":"v0.15.0-alpha.0"}]'; }
assert_eq "resolve_version falls back to prerelease" "v0.15.0-alpha.0" "$(resolve_version)"

# Case C: explicit pin wins, no network consulted.
http_effective_url() { printf '%s' "UNUSED"; }
http_body() { printf '%s' "UNUSED"; }
assert_eq "resolve_version honors GMOUNTIE_VERSION" "v0.9.9" \
  "$(GMOUNTIE_VERSION=v0.9.9 resolve_version)"
```

- [ ] **Step 2: Run to verify it fails**

Run: `sh scripts/install/install_test.sh`
Expected: FAIL — `resolve_version: not found`.

- [ ] **Step 3: Implement `resolve_version`**

In `website/static/install.sh`, add after `sha256_of`:

```sh
# resolve_version -> tag to install.
# Order: explicit GMOUNTIE_VERSION; else the stable channel (releases/latest
# redirect); else the newest release from the API (prereleases included).
resolve_version() {
  if [ -n "${GMOUNTIE_VERSION:-}" ]; then
    printf '%s' "$GMOUNTIE_VERSION"
    return 0
  fi
  _eff=$(http_effective_url "$GH/$REPO/releases/latest")
  if _tag=$(tag_from_latest_url "$_eff"); then
    printf '%s' "$_tag"
    return 0
  fi
  _json=$(http_body "$API/repos/$REPO/releases")
  if _tag=$(tag_from_releases_json "$_json"); then
    printf '%s' "$_tag"
    return 0
  fi
  die "could not resolve a release version (set GMOUNTIE_VERSION to pin one)"
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `sh scripts/install/install_test.sh`
Expected: PASS.

- [ ] **Step 5: Reset the overridden wrappers so later tests are unaffected**

Confirm the IO overrides in Step 1 are the **last** uses of `http_effective_url`/`http_body` in the file (they are appended at the end of the test list). No action needed if Task 8 is the final IO-dependent test block; otherwise move these overrides into a subshell. Re-run to confirm:

Run: `sh scripts/install/install_test.sh`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add website/static/install.sh scripts/install/install_test.sh
git commit -m "feat(install): resolve version, preferring stable then newest prerelease"
```

---

### Task 9: `choose_bindir` (install-dir decision, pure)

**Files:**
- Modify: `website/static/install.sh`
- Modify: `scripts/install/install_test.sh`

- [ ] **Step 1: Add failing tests**

In `scripts/install/install_test.sh`, add before `# ---- END TESTS ----`:

```sh
# choose_bindir <BIN_DIR env> <usrlocal_writable:1|0> <have_sudo:1|0>
assert_eq "choose_bindir honors BIN_DIR" "/opt/bin" "$(choose_bindir /opt/bin 0 0)"
assert_eq "choose_bindir writable usrlocal" "/usr/local/bin" "$(choose_bindir '' 1 0)"
assert_eq "choose_bindir sudo path" "sudo:/usr/local/bin" "$(choose_bindir '' 0 1)"
assert_eq "choose_bindir user fallback" "$HOME/.local/bin" "$(choose_bindir '' 0 0)"
```

- [ ] **Step 2: Run to verify it fails**

Run: `sh scripts/install/install_test.sh`
Expected: FAIL — `choose_bindir: not found`.

- [ ] **Step 3: Implement `choose_bindir`**

In `website/static/install.sh`, add after `resolve_version`:

```sh
# choose_bindir <BIN_DIR> <usrlocal_writable:1|0> <have_sudo:1|0> -> target dir.
# A "sudo:" prefix signals the caller to install via sudo into /usr/local/bin.
choose_bindir() {
  if [ -n "$1" ]; then printf '%s' "$1"; return 0; fi
  if [ "$2" = 1 ]; then printf '/usr/local/bin'; return 0; fi
  if [ "$3" = 1 ]; then printf 'sudo:/usr/local/bin'; return 0; fi
  printf '%s/.local/bin' "$HOME"
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `sh scripts/install/install_test.sh`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add website/static/install.sh scripts/install/install_test.sh
git commit -m "feat(install): pure install-dir selection"
```

---

### Task 10: `main` — wire the install flow

No new unit test (this is IO orchestration covered by the smoke job in Task 12). Keep each helper call guarded.

**Files:**
- Modify: `website/static/install.sh`

- [ ] **Step 1: Replace the placeholder `main`**

In `website/static/install.sh`, replace the existing `main()` body with:

```sh
main() {
  have tar || die "tar is required"
  { have curl || have wget; } || die "curl or wget is required"
  { have sha256sum || have shasum; } || die "sha256sum or shasum is required"

  os=$(normalize_os "$(uname -s)") || die "unsupported OS: $(uname -s)"
  arch=$(normalize_arch "$(uname -m)") || die "unsupported arch: $(uname -m)"

  tag=$(resolve_version)
  printf 'install: gmountie %s (%s/%s)\n' "$tag" "$os" "$arch" >&2

  base="$GH/$REPO/releases/download/$tag"
  checksums=$(http_body "$base/checksums.txt") || die "failed to fetch checksums.txt"
  archive=$(pick_archive "$checksums" "$os" "$arch") \
    || die "no $os/$arch archive in this release"
  want=$(sha_for "$checksums" "$archive") || die "no checksum for $archive"

  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  http_body "$base/$archive" > "$tmp/$archive" || die "download failed"

  got=$(sha256_of "$tmp/$archive")
  [ "$got" = "$want" ] || die "checksum mismatch for $archive (got $got, want $want)"

  if have cosign; then
    if http_body "$base/checksums.txt.pem" > "$tmp/checksums.txt.pem" 2>/dev/null \
       && http_body "$base/checksums.txt.sig" > "$tmp/checksums.txt.sig" 2>/dev/null; then
      printf '%s' "$checksums" > "$tmp/checksums.txt"
      # Pin the issuer to GitHub Actions' OIDC and scope the signer identity to
      # this repo, so a valid signature from some *other* Fulcio identity is not
      # accepted. (Permissive on workflow file / ref to avoid brittleness across
      # release-workflow renames; tighten to release.yml@refs/tags/$tag later.)
      cosign verify-blob \
        --certificate "$tmp/checksums.txt.pem" \
        --signature "$tmp/checksums.txt.sig" \
        --certificate-oidc-issuer https://token.actions.githubusercontent.com \
        --certificate-identity-regexp "^https://github.com/gMountie/gMountie/" \
        "$tmp/checksums.txt" >/dev/null 2>&1 \
        || die "cosign signature verification failed"
      printf 'install: cosign signature verified\n' >&2
    fi
  else
    printf 'install: cosign not found; skipping signature verification (checksum verified)\n' >&2
  fi

  tar -xzf "$tmp/$archive" -C "$tmp" "$BIN_NAME" \
    || die "could not extract $BIN_NAME from archive"

  usrlocal_writable=0; [ -w /usr/local/bin ] && usrlocal_writable=1
  have_sudo=0; have sudo && have_sudo=1
  dir=$(choose_bindir "${BIN_DIR:-}" "$usrlocal_writable" "$have_sudo")

  case "$dir" in
    sudo:*)
      target=${dir#sudo:}
      printf 'install: using sudo to write %s/%s\n' "$target" "$BIN_NAME" >&2
      sudo install -m 0755 "$tmp/$BIN_NAME" "$target/$BIN_NAME" || die "install failed"
      ;;
    *)
      mkdir -p "$dir"
      install -m 0755 "$tmp/$BIN_NAME" "$dir/$BIN_NAME" || die "install failed"
      target=$dir
      ;;
  esac

  printf 'install: installed %s to %s\n' "$BIN_NAME" "$target/$BIN_NAME" >&2
  case ":$PATH:" in
    *":$target:"*) : ;;
    *) printf 'install: %s is not on PATH — add it: export PATH="%s:$PATH"\n' "$target" "$target" >&2 ;;
  esac
  "$target/$BIN_NAME" version >&2 || true
  printf 'install: done — run `%s --help` to get started\n' "$BIN_NAME" >&2
}
```

- [ ] **Step 2: Verify the unit suite still passes**

Run: `sh scripts/install/install_test.sh`
Expected: PASS (no tested pure function changed).

- [ ] **Step 3: Commit**

```bash
git add website/static/install.sh
git commit -m "feat(install): wire download, verify, extract, and install flow"
```

---

### Task 11: shellcheck clean

**Files:**
- Modify: `website/static/install.sh` (only if shellcheck flags anything)

- [ ] **Step 1: Run shellcheck**

Run: `shellcheck -s sh website/static/install.sh`
(If `shellcheck` is not installed: `sudo apt-get install -y shellcheck` on Debian/Ubuntu, or `brew install shellcheck` on macOS.)
Expected: no output (clean). Common fixes if flagged: quote expansions, replace any non-POSIX construct. Apply minimal fixes; do **not** change behavior.

- [ ] **Step 2: Re-run unit tests after any fix**

Run: `sh scripts/install/install_test.sh`
Expected: PASS.

- [ ] **Step 3: Commit (only if changes were needed)**

```bash
git add website/static/install.sh
git commit -m "style(install): shellcheck-clean the installer"
```

---

### Task 12: CI — unit + shellcheck + smoke jobs

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add the jobs**

In `.github/workflows/ci.yml`, add these two jobs under `jobs:` (sibling to `lint`, matching the file's 2-space indentation):

```yaml
  install-script:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - uses: actions/checkout@v6
      - name: shellcheck
        run: shellcheck -s sh website/static/install.sh
      - name: unit tests
        run: sh scripts/install/install_test.sh

  install-smoke:
    # End-to-end: run the real installer against the live release artifacts and
    # confirm the binary works. Guards against goreleaser name-template / artifact
    # drift that the unit tests (which use a fixture) cannot see.
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - uses: actions/checkout@v6
      - name: install via script
        env:
          BIN_DIR: ${{ github.workspace }}/bin
        run: sh website/static/install.sh
      - name: verify binary
        run: ${{ github.workspace }}/bin/gmountie version
```

(`shellcheck` is preinstalled on `ubuntu-latest` runners.)

- [ ] **Step 2: Validate the workflow YAML locally**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo OK`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci(install): shellcheck, unit tests, and a live smoke install"
```

---

### Task 13: Push, open PR, and document the rollout

**Files:** none (process task).

- [ ] **Step 1: Push the branch**

```bash
git push -u origin feat/install-script
```

- [ ] **Step 2: Open the PR with the rollout checklist in the body**

```bash
gh pr create --repo gMountie/gMountie --base master --head feat/install-script \
  --title "feat(install): get.gmountie.dev install script" \
  --body "$(cat <<'EOF'
Implements `curl -fsSL https://get.gmountie.dev | sh`. The script installs the
`gmountie` CLI from GitHub Releases. Design: docs/superpowers/specs/2026-06-09-get-install-script-design.md

## Post-merge rollout (ordering matters — the redirect target only exists after a docs deploy)
1. Merge to master.
2. Trigger the docs build: `gh workflow run docs.yml` (deploys the default branch).
3. Confirm `https://docs.gmountie.dev/install.sh` is live and correct.
4. Cloudflare (gmountie.dev zone):
   - Add a `get` DNS record, **proxied (orange-cloud)**.
   - Add a Redirect Rule: `get.gmountie.dev/*` -> `302 https://docs.gmountie.dev/install.sh`.
5. Verify end-to-end on Linux and macOS: `curl -fsSL https://get.gmountie.dev | sh`.

Note: until a stable release is tagged, the installer serves the newest `-alpha`;
it auto-prefers stable once one exists (no script change).
EOF
)"
```

- [ ] **Step 3: Confirm CI is green on the PR**

Run: `gh pr checks --repo gMountie/gMountie --watch`
Expected: all checks pass (notably `install-script` and `install-smoke`).
