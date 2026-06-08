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

# normalize_arch <uname -m output> -> amd64|arm64 (exit 1 if unsupported)
normalize_arch() {
  case "$1" in
    x86_64 | amd64) printf 'amd64' ;;
    aarch64 | arm64) printf 'arm64' ;;
    *) return 1 ;;
  esac
}

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

# sha_for <checksums.txt contents> <filename> -> expected sha256 hex. exit 1 if absent.
sha_for() {
  _sha=$(printf '%s\n' "$1" | awk -v f="$2" '$NF == f {print $1}' | head -n1)
  [ -n "$_sha" ] || return 1
  printf '%s' "$_sha"
}

# tag_from_latest_url <effective url of /releases/latest> -> tag.
# A real stable release redirects to .../releases/tag/<tag>; with no stable
# release GitHub serves .../releases (no /tag/), so we report failure.
tag_from_latest_url() {
  case "$1" in
    */releases/tag/*) printf '%s' "${1##*/releases/tag/}" ;;
    *) return 1 ;;
  esac
}

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

# choose_bindir <BIN_DIR> <usrlocal_writable:1|0> <have_sudo:1|0> -> target dir.
# A "sudo:" prefix signals the caller to install via sudo into /usr/local/bin.
choose_bindir() {
  if [ -n "$1" ]; then printf '%s' "$1"; return 0; fi
  if [ "$2" = 1 ]; then printf '/usr/local/bin'; return 0; fi
  if [ "$3" = 1 ]; then printf 'sudo:/usr/local/bin'; return 0; fi
  printf '%s/.local/bin' "$HOME"
}

main() {
  die "main not implemented yet"
}

if [ "${INSTALL_SH_SOURCED:-0}" != "1" ]; then
  main "$@"
fi
