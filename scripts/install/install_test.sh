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

assert_eq "sha_for linux/amd64" \
  "1111111111111111111111111111111111111111111111111111111111111111" \
  "$(sha_for "$checksums" gmountie_0.15.0-alpha.0_linux_amd64.tar.gz)"
assert_fails "sha_for missing file" sha_for "$checksums" nope.tar.gz

assert_eq "tag_from_latest_url stable tag" "v1.2.0" \
  "$(tag_from_latest_url https://github.com/gMountie/gMountie/releases/tag/v1.2.0)"
# When no stable release exists the redirect lands on the list page -> no tag.
assert_fails "tag_from_latest_url list page" \
  tag_from_latest_url https://github.com/gMountie/gMountie/releases

# Minimal shape of GET /repos/.../releases (newest first), no jq available.
releases_json='[{"tag_name":"v0.15.0-alpha.0","name":"x"},{"tag_name":"v0.14.0-alpha.0"}]'
assert_eq "tag_from_releases_json newest" "v0.15.0-alpha.0" \
  "$(tag_from_releases_json "$releases_json")"
assert_fails "tag_from_releases_json empty" tag_from_releases_json '[]'

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

# ---- END TESTS ----

if [ "$fails" -gt 0 ]; then
  printf '\n%d test(s) failed\n' "$fails"; exit 1
fi
printf '\nall tests passed\n'
