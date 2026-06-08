# `get.gmountie.dev` install script — design

- **Date:** 2026-06-09
- **Status:** Approved (brainstorming), pending implementation plan
- **Repo:** OSS `gMountie/gMountie` (this is not a cloud-repo change)
- **Branch / worktree:** `feat/install-script`

## Goal

Make `curl -fsSL https://get.gmountie.dev | sh` actually install the `gmountie`
CLI. The cloud console already advertises this exact one-liner
(`gMountie-cloud/web/src/lib/api.ts:12`, shown in the Welcome onboarding flow),
but the endpoint has no infrastructure behind it.

## Why this lives in the OSS repo

The script installs only the **public** `gmountie` CLI from GitHub Releases.
Nothing cloud-specific touches it: the hosted product lives on `gmountie.cloud`,
and cloud credentials are supplied separately via `GMOUNTIE_CREDENTIALS` *after*
install. So the install endpoint belongs on the `.dev` (OSS) domain, and the
script + its CI live in the public repo. No closed material is involved, and the
cloud repo needs no edits (it already points at the URL).

## Domain facts (verified)

- DNS zone `gmountie.dev` is on **Cloudflare** (proxied).
- Only `docs.gmountie.dev` is currently hosted — GitHub Pages, built from this
  repo's Docusaurus site (`website/`), `CNAME` = `docs.gmountie.dev`.
- The docs Pages deploy (`.github/workflows/docs.yml`) runs on
  `release: published` **or** manual `workflow_dispatch` (which deploys the
  default branch). Anything in `website/static/` is published verbatim at the
  site root.
- GitHub repo is `github.com/gMountie/gMountie` (case-insensitive in URLs).
- Release reality: **no stable release exists** — every tag is a `-alpha`
  prerelease (latest at time of writing: `v0.15.0-alpha.0`). Consequently the
  `…/releases/latest` redirect resolves to the *list page*, not a tag.

## Decisions (locked during brainstorming)

| Decision | Choice |
|---|---|
| Hosting | **Cloudflare Redirect Rule** → static `install.sh` on the docs Pages site. No Worker/compute in the install path. |
| Script source of truth | `website/static/install.sh` (the published artifact *is* the canonical file). |
| Install location | `/usr/local/bin` if writable or `sudo` available; else fall back to `~/.local/bin` (warn if not on `PATH`). `BIN_DIR` env override. |
| Version resolution | **Prefer stable, fall back to newest prerelease.** Auto-switches to stable-only once a stable tag is cut. `GMOUNTIE_VERSION` pins explicitly. |
| Archive name | **Read from `checksums.txt`**, never reconstructed from the goreleaser name-template. |
| Verification | SHA256 against `checksums.txt` (**hard fail** on mismatch). If `cosign` is on `PATH`, also verify the keyless signature; otherwise print a one-line note and continue. |
| Shell | POSIX `sh` (the one-liner pipes to `sh`, not bash). |

## Architecture

Two independent pieces:

1. **`install.sh`** — a POSIX shell script, committed at
   `website/static/install.sh`, published at `https://docs.gmountie.dev/install.sh`.
2. **Cloudflare Redirect Rule** — `get.gmountie.dev/*` →
   `302 https://docs.gmountie.dev/install.sh`. `curl -fsSL` follows it (`-L`).

The redirect rule is deliberately the *only* bespoke hosting element: if
Cloudflare can serve the redirect, `curl -L` lands on a static file on GitHub
Pages' CDN. Upgrading later to a content-negotiating Worker (curl → script,
browser → docs page) needs no change to the script or the `curl | sh`
one-liner — just swap the rule for a Worker pointing at the same `install.sh`.

## The script: `install.sh`

POSIX `sh`. Top-level flow:

1. **Preconditions.** Require `tar` and a downloader (`curl` or `wget`); require
   a SHA256 tool (`sha256sum` or macOS `shasum -a 256`). Fail early with a
   readable message naming what's missing.

2. **Platform detection** via `uname`:
   - OS: `Linux → linux`, `Darwin → darwin`. Anything else (e.g. Windows/MSYS)
     → clear error pointing to the releases page + docs, then exit non-zero.
   - Arch: `x86_64 → amd64`, `aarch64`/`arm64 → arm64`. Unknown arch → error.
   - These map to the goreleaser matrix (`linux`/`darwin` × `amd64`/`arm64`).

3. **Version resolution** (overridable by `GMOUNTIE_VERSION`):
   - First try the stable channel: resolve the effective URL of
     `https://github.com/gMountie/gMountie/releases/latest`. If it ends in
     `/tag/<something>`, use `<something>` as the tag.
   - If it resolves to the bare `/releases` list page (no stable release), fall
     back to the GitHub API:
     `https://api.github.com/repos/gMountie/gMountie/releases` and take the
     first element's `tag_name` (the API returns newest-first, including
     prereleases). Parse the first `"tag_name":` line without `jq`.
   - The fallback makes the script work **today** (serves the newest alpha) and
     **automatically** prefer stable once a stable tag exists — no script edit.
   - Unauthenticated API calls are rate-limited to 60/hr/IP, which is ample for
     an installer.

4. **Resolve the archive name from `checksums.txt`.** Download
   `…/releases/download/<tag>/checksums.txt`, then select the line whose
   filename matches `*_<os>_<arch>.tar.gz` and extract that exact filename. This
   sidesteps goreleaser name-template details entirely (`ProjectName` casing,
   the `v`-prefix that `.Version` strips, etc.) and pairs naturally with the
   checksum step.

5. **Download** the archive to a temp dir (created with `mktemp -d`, removed via
   a `trap` on `EXIT`).

6. **Verify:**
   - Recompute the archive's SHA256 and compare against its line in
     `checksums.txt`. **Mismatch → abort** (do not install).
   - If `cosign` is present, fetch the checksum signature/cert artifacts and run
     `cosign verify-blob` (keyless). On failure → abort. If `cosign` is absent,
     print one line noting signature verification was skipped, and continue.

7. **Install:**
   - Extract `gmountie` from the tarball.
   - Choose `BIN_DIR`: explicit `BIN_DIR` env wins; else `/usr/local/bin` if
     writable; else `/usr/local/bin` via `sudo` if `sudo` is available and
     interactive; else `~/.local/bin`.
   - Move the binary into place (overwrites an existing install — acts as
     upgrade). Print old → new version when replacing.
   - If the chosen dir is not on `PATH`, print a warning with the line to add.

8. **Finish.** Print the installed version (`gmountie version`) and a next-step
   hint (`gmountie --help`; for cloud users, enroll a device and
   `export GMOUNTIE_CREDENTIALS=…`).

### Environment knobs

- `GMOUNTIE_VERSION` — install a specific tag instead of resolved latest.
- `BIN_DIR` — force the install directory.

## CI / safety (this repo)

- **`shellcheck`** on `website/static/install.sh`, added to the existing lint
  path (`.github/workflows/ci.yml`). The repo has no shellcheck step today.
- **Smoke test (recommended):** a CI job that runs the script end-to-end in a
  clean Linux container and asserts `gmountie version` works. This catches
  goreleaser name-template / artifact drift before users hit it. Because version
  resolution prefers real published releases, this exercises the live release
  artifacts.

## Rollout sequence (ordering matters)

The redirect target does not exist until a docs deploy publishes
`install.sh`, so sequence to avoid a window where the live one-liner 404s:

1. Merge the PR (script + CI) to `master`.
2. Trigger the docs build (`workflow_dispatch` on `docs.yml`, which deploys the
   default branch) — or wait for the next release.
3. Confirm `https://docs.gmountie.dev/install.sh` is live and correct.
4. **Then** add the Cloudflare config:
   - A `get` DNS record, **proxied (orange-cloud)** — a DNS-only record has no
     edge to run the rule.
   - A Redirect Rule: `get.gmountie.dev/*` → `302
     https://docs.gmountie.dev/install.sh`.
5. Verify `curl -fsSL https://get.gmountie.dev | sh` end-to-end on Linux + macOS.

If the Cloudflare zone is Terraform-managed, codify the DNS record + redirect
rule there instead of clicking; otherwise this is a documented manual ops step.

## Non-goals (YAGNI)

- No Worker / content negotiation / install telemetry in v1 (clean upgrade path
  preserved if wanted later).
- No Windows support (not in the goreleaser build matrix).
- No package-manager distribution (Homebrew tap, apt/yum) — separate effort.
- No changes to the cloud repo.

## Open coordination points

- **Cloudflare changes** are an ops step outside this repo's CI. Confirm whether
  the zone is Terraform-managed so they can be codified.
- The smoke-test job depends on the release artifacts being intact; if a release
  is mid-publish it could flake — gate or retry accordingly.
