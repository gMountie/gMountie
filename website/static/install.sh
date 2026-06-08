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
