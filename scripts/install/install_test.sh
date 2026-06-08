#!/bin/sh
# Dependency-free POSIX unit tests for website/static/install.sh.
# Sources the script (guarded by INSTALL_SH_SOURCED) and exercises its
# pure functions — no network, no system mutation. Run: sh scripts/install/install_test.sh
set -u

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
INSTALL_SH_SOURCED=1 . "$here/../../website/static/install.sh"

checksums=$(cat "$here/testdata/checksums.txt")

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

assert_eq "normalize_arch amd64 (x86_64)"  "amd64" "$(normalize_arch x86_64)"
assert_eq "normalize_arch amd64 (amd64)"   "amd64" "$(normalize_arch amd64)"
assert_eq "normalize_arch arm64 (aarch64)" "arm64" "$(normalize_arch aarch64)"
assert_eq "normalize_arch arm64 (arm64)"   "arm64" "$(normalize_arch arm64)"
assert_fails "normalize_arch rejects i386" normalize_arch i386

assert_eq "pick_archive linux/amd64" \
  "gmountie_0.15.0-alpha.0_linux_amd64.tar.gz" \
  "$(pick_archive "$checksums" linux amd64)"
assert_eq "pick_archive darwin/arm64" \
  "gmountie_0.15.0-alpha.0_darwin_arm64.tar.gz" \
  "$(pick_archive "$checksums" darwin arm64)"
assert_fails "pick_archive unknown platform" pick_archive "$checksums" plan9 mips

# ---- END TESTS ----

if [ "$fails" -gt 0 ]; then
  printf '\n%d test(s) failed\n' "$fails"; exit 1
fi
printf '\nall tests passed\n'
