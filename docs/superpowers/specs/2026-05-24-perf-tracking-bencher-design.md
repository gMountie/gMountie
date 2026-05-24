# Release-Gated Performance Tracking with Bencher

**Status:** Design approved 2026-05-24; in-repo pipeline implemented 2026-05-25 (runner / ARC / Bencher provisioning tracked in the infra repo).

**Builds on:**
- The existing benchstat-shaped harness at `test/e2e/perf/` (sequential / random IO + metadata benchmarks, each spinning up an in-process server + FUSE mount via `test/e2e/utils`).
- The `GMOUNTIE_BENCH_TCP=1` transport variant (real loopback TCP so `tc netem` shaping bites) and the `GMOUNTIE_BENCH_CACHE=1` cache-on variant.
- The `tc netem` loopback scripts (`scripts/start-slow-loopback.sh` / `stop-slow-loopback.sh`).
- The release workflow `.github/workflows/release.yml` (`workflow_dispatch` with a `type` input: production / alpha / snapshot; computes the tag via `svu` and pushes it).

**Closes:** The "no automated lifecycle perf tracking" gap. After this, every alpha/production release records a comparable, machine-pinned perf snapshot over LAN and WAN profiles into Bencher, with regression alerts and an over-time dashboard — so we can see what improves or degrades performance across the project's lifecycle.

## Goal

Turn the existing manual, phase-named perf measurement (hand-run on a VM, results committed as `.txt`/`.md` files into `docs/perf/`) into an automated, reproducible, release-gated time series that is **comparable across the project's lifecycle**.

Concretely:

1. **Comparability is enforced, not assumed.** Runs execute on a single fixed machine (a self-hosted runner) so the hardware floor never moves. Each run also captures a *substrate fingerprint* (raw CPU / disk / network, measured without gMountie) so floor drift is detectable and results can be expressed as overhead ratios.
2. **LAN and WAN are first-class, codified profiles** — not a `tc` command you remember to type. Both run over the real loopback TCP transport; WAN adds a version-controlled `netem` profile.
3. **Results live off-repo in Bencher**, which owns storage, the over-time dashboard, and threshold-based regression alerting. `master` is never touched by the perf pipeline.
4. **Runs only on release** (alpha + production), matching how perf signal is actually consumed — per-release, not per-commit.

## Non-goals

- **GitHub-hosted runners.** Rejected: the hardware under `ubuntu-latest` rotates between jobs, which violates the comparability requirement. We use a self-hosted runner running as a single node-pinned Pod in the user's k8s cluster (see Runner).
- **Portable / ephemeral containerized runs.** Rejected: a container's cgroup limit is a ceiling, not a floor, so running the suite on *varying* hosts — or on ARC autoscaling runners that reschedule per job — does not give comparability. The runner *is* a container Pod (see Runner), but comparability comes from pinning that Pod to one fixed node with dedicated resources, **not** from the container itself. We accept the one trade a container makes versus a VM — it shares the node's kernel, and FUSE is kernel-sensitive — with the kubevirt VM as the documented fallback if that proves to matter (see Risks).
- **Real cross-internet WAN runs.** The public internet is non-reproducible minute-to-minute, so it cannot anchor a tracked series. WAN is *simulated* via `netem`; the network fingerprint confirms the profile took effect. A real-WAN reality-check can be a future ad-hoc exercise, not part of the series.
- **Blocking/rolling back a release on regression.** The perf job runs *after* the tag is cut, so it is advisory: Bencher alerts, the release stands.
- **Self-hosted Bencher (for now).** Start on Bencher Cloud (free for public projects). Self-hosting on the existing k8s is a documented future migration, not part of this work.
- **Replacing the local harness.** `task perf:bench*` / `perf:diff` stay for ad-hoc local work. Bencher layers on top of the same benchmarks.
- **Per-PR perf gating.** Out of scope; release-cadence only.

## Key decisions and rationale

| Decision | Choice | Why |
| --- | --- | --- |
| Runner | Self-hosted, as a single **node-pinned** k8s Pod (long-lived, *not* ARC ephemeral) | Easier declarative in-cluster management; comparability comes from node affinity + taint + Guaranteed QoS + local storage, not from the container. |
| Containerization | Container Pod pinned to a fixed node | Accepts the shared-kernel trade vs a VM (FUSE is kernel-sensitive) for management simplicity; kubevirt VM is the fallback if substrate variance proves too broad. |
| Trigger | `perf` job in `release.yml`, `needs: [release]`, gated `type != 'snapshot'` | Runs exactly on alpha/production releases, after the tag exists. |
| Network profiles | `lan` (no shaping) + `wan` (`delay 25ms jitter 5ms rate 100Mbit`), both over loopback TCP | Codified and version-controlled; bufconn can't see shaping so the TCP transport is mandatory. |
| Comparability guard | Per-run substrate fingerprint (CPU/disk/network) | Detects fixed-host drift (disk filling, noisy hypervisor neighbor, kernel upgrade); enables normalized overhead metrics. |
| Storage/dashboard/regression | Bencher Cloud (hosted) | Purpose-built continuous benchmarking; `testbed` model maps to the fixed VM; off-repo; thresholds replace hand-rolled benchstat diffing. |
| Metric ingestion | Bencher Metric Format (BMF) JSON via the `json` adapter | The `go_bench` adapter is latency-only; BMF lets us report latency **and** throughput **and** normalized ratios as custom measures. |
| Regression policy | Alert only (advisory) | Job runs post-tag; loud signal without gating the release. |

## Architecture

The pipeline is a single CI job that orchestrates four existing-or-new pieces: the substrate probe, the bench suite (already present), a BMF emitter (new), and the Bencher upload (new). Nothing is committed back to the repo.

### Runner (single pinned Pod)

The self-hosted GitHub Actions runner runs as a **single long-lived Pod** in the user's k8s cluster (a plain Deployment, or an ARC `gha-runner-scale-set` pinned `min=max=1` — **never** ephemeral autoscaling, which reschedules per job and destroys comparability). To make the Pod a stable substrate it is constrained as:

- **Node-pinned** — `nodeSelector`/affinity to one specific node, plus a taint on that node so nothing else schedules there. A reschedule onto a different node silently changes the CPU/disk floor.
- **Guaranteed QoS + dedicated cores** — `requests == limits`, with the kubelet static CPU manager policy so the Pod gets pinned cores rather than shared quota.
- **Local storage, not a network PV** — the volume backing dir and FUSE data live on a local PV / hostPath on that node's local disk, so disk numbers reflect a stable local device, not a shared (noisy, variable) network backend.
- **Privileged + `/dev/fuse`** — `securityContext.privileged` (or `SYS_ADMIN` + a `/dev/fuse` device plugin) so the in-process FUSE mount works. Consistent with the perf namespace already being PSA `privileged`.
- **Outbound HTTPS** to `bencher.dev` for the upload, and **passwordless sudo for `tc`** for the WAN `netem` profile.

This is the explicit accepted trade: a container Pod shares the node's kernel (FUSE is kernel-sensitive) and has more drift sources (node kernel upgrades, accidental reschedule) than a VM. The substrate fingerprint is the guard that surfaces such drift; if it shows variance too broad to trust, the fallback is to replace the Pod with a kubevirt VM (registered as a new testbed — see below).

### Bencher data model mapping

- **Project:** `gmountie` (one Bencher project).
- **Testbed:** `gmountie-perf-pod` — the fixed pinned runner Pod. The testbed *is* the comparability anchor; we keep one testbed for the one substrate and do **not** fragment it per network profile. If the substrate is later replaced (e.g. Pod → kubevirt VM), register it as a **new testbed** (`gmountie-perf-vm`) so the substrate change is an explicit series break rather than a silent step in the data.
- **Branch:** `master` — the single development line the series lives on. In Bencher, a Branch *is* the time series; datapoints on one Branch form the over-time chart. We therefore pass `--branch master` **explicitly** to `bencher run` and identify each datapoint by commit hash / release version (`--hash $GITHUB_SHA`, version-stamped with the tag). Passing the *tag* as the branch would create a fresh single-point Branch per release and destroy the series; and a checked-out release tag is a detached HEAD, where `bencher run`'s branch auto-detection is unreliable — so the explicit `--branch` is load-bearing, not cosmetic. (Confirm the exact version/hash flags against the `bencher run` reference during implementation.)
- **Benchmark:** existing bench name suffixed with the profile, e.g. `SeqRead64MiB/lan`, `SeqRead64MiB/wan`, `OpenStatClose/wan`. This keeps LAN and WAN as distinct tracked series on the same machine.
- **Measures (per benchmark):**
  - `latency` — ns/op.
  - `throughput` — MB/s (from `b.SetBytes`; absent for metadata benchmarks).
  - `throughput_pct_of_raw` — normalized: achieved throughput as a percentage of the substrate ceiling for that profile (read benches vs raw disk read / link bandwidth; write benches vs raw disk write). Survives moderate floor drift.
  - (Optional, low priority) `alloc_bytes`, `allocs` from `b.ReportAllocs`.
- **Substrate benchmarks (drift visibility):** the raw fingerprint is *also* uploaded as its own benchmarks — `_substrate/disk_seq_read`, `_substrate/disk_seq_write`, `_substrate/disk_rand_4k_iops`, `_substrate/cpu_compute`, `_substrate/net_rtt_lan`, `_substrate/net_rtt_wan` — so the *floor itself* is visible on the dashboard over time, not only folded into the normalization math. This is what actually delivers the "detect substrate drift" guarantee: a `throughput_pct_of_raw` ratio self-normalizes and would stay healthy even if the disk silently degraded, so the raw substrate series is required to *see* that the machine moved. (These can carry their own Bencher thresholds to alert on floor drift directly.)

### Network profiles

A small version-controlled profile definition (a shell snippet or YAML consumed by the job) names the two profiles:

- `lan`: no `netem` qdisc. Loopback TCP only.
- `wan`: `tc qdisc … netem delay 25ms jitter 5ms rate 100Mbit` applied to `lo` for the duration of the WAN pass, then removed (reuse / generalize `scripts/start-slow-loopback.sh` + `stop-slow-loopback.sh`).

The job always removes any qdisc on teardown (and defensively at startup) so shaping never leaks between passes or runs.

### Per-run sequence (the CI job)

1. **Checkout** the freshly-cut release tag (`needs: [release]`; the release job exposes the computed tag as a job output, or the perf job resolves it via `git describe --tags --abbrev=0`).
2. **Verify pinned toolchain**: Go (from `go.mod`), `benchstat`, `fio`, `iperf3` at fixed versions (pre-installed on the persistent runner; the job asserts versions and fails loudly on mismatch).
3. **Substrate fingerprint** (no gMountie in the path):
   - CPU: a fixed-work compute probe → ops/sec (a tiny dedicated Go benchmark, or `sysbench cpu`).
   - Disk: `fio` with a committed job file straight to the volume's backing directory's filesystem → seq read/write MB/s + 4 KiB random IOPS.
   - Network, per profile: apply the profile's `netem`, then `iperf3` + `ping` over `lo`. For `wan`, assert measured RTT ≈ target (within tolerance) so a mis-applied profile fails the run rather than silently mislabeling data.
   - Emit `substrate.json`.
4. **Bench suite, per profile** (`lan` then `wan`), over real loopback TCP (`GMOUNTIE_BENCH_TCP=1`), with a higher `COUNT` than the local VM default for statistical honesty (tune during implementation; benchstat wants ≥6–10). Capture raw `go test -bench` text for archival as a CI artifact.
5. **BMF emission**: a post-processor combines the bench output with `substrate.json` into one BMF JSON document — the gMountie benchmarks (per benchmark × profile, absolute + normalized) **and** the `_substrate/*` benchmarks carrying the raw fingerprint values.
6. **Upload**: the perf job (i.e. the runner Pod itself) runs `bencher run --adapter json --project gmountie --branch master --testbed gmountie-perf-pod --hash $GITHUB_SHA --token $BENCHER_API_TOKEN …` ingesting the BMF document, version-stamped with the release tag. The Pod pushes directly — it already holds the data and has outbound network. Bencher stores it, updates the dashboard, and evaluates thresholds.
7. **Artifacts**: attach the raw bench text + `substrate.json` + BMF JSON as GitHub Actions artifacts (debugging / audit trail). These expire with normal artifact retention; Bencher is the durable home for both the gMountie *and* substrate series.

### Regression detection

Bencher **thresholds** (per measure / branch / testbed) replace the hand-rolled benchstat comparison. Configured to **alert only** — surfaced in the Bencher console and (optionally) as a workflow annotation / job-summary line. The perf job does not fail the release on a breach.

## Components to build / modify

**New:**
- `.github/workflows/release.yml` — add a `perf` job (`needs: [release]`, `if: inputs.type != 'snapshot'`, `runs-on: [self-hosted, gmountie-perf]`).
- A BMF emitter — a small Go program under `test/e2e/perf/` (e.g. `cmd/bmf/` or a `-json`-style flag on a runner) that reads bench output + `substrate.json` and writes BMF JSON. Go (not shell) so the parsing/normalization logic is testable with a suite.
- Substrate probe scripts/fio job files (committed, version-controlled) under `test/e2e/perf/substrate/` (or `scripts/perf/`).
- A profile definition file (`lan`/`wan` + the `netem` spec) consumed by both the job and local use.
- `docs/perf/README.md` — document the Bencher pipeline, the testbed, how to read the dashboard, and how to reproduce a run locally.

**Modified:**
- `Taskfile.yaml` — optional `perf:bmf` / `perf:substrate` targets so the pipeline steps are runnable locally and from CI uniformly.
- `scripts/start-slow-loopback.sh` — generalize/parametrize so the `wan` profile (`delay 25ms jitter 5ms rate 100Mbit`) is expressed once and shared.

**Infrastructure (operational, owned by the separate ARC / runner-infra repo — NOT built here):**

This repo assumes the runner already exists with these properties; provisioning lives elsewhere. Listed so the pipeline's assumptions are explicit:
- The self-hosted ARC runner is a single node-pinned Pod (label `gmountie-perf`) per the Runner section, with `fio`, `iperf3`, `benchstat`, Go, `tc` (`iproute2`), `fuse3`, and the pinned `bencher` CLI **baked into the runner image**.
- The runner can run `tc` (privileged Pod grants `NET_ADMIN`; or passwordless sudo for `tc`) — needed for the `wan` `netem` qdisc on the Pod's `lo`.
- The Bencher Cloud project `gmountie` + testbed `gmountie-perf-pod` exist, and `BENCHER_API_TOKEN` is available to the workflow as a secret. **Token identity:** recommend a dedicated Bencher bot/service account that owns the project (so rotation never touches a personal account); a personal-account token works for a solo start. *(Open: confirm which identity.)* Secret exposure to the self-hosted Pod is acceptable because the perf job fires only on manual `workflow_dispatch` release, never on untrusted fork PRs.

**In-repo scope (what this spec's implementation actually delivers):** the `perf` job in `release.yml`, the BMF emitter, the substrate probes + profile definitions, the Taskfile targets, and the docs. Everything under "New" / "Modified" above.

## Data flow

```
release.yml (release job: svu tag + goreleaser + push tag)
        │  needs, if type != snapshot
        ▼
perf job on [self-hosted, gmountie-perf]  (single node-pinned Pod)
   checkout tag
   verify toolchain versions
        │
        ├─ substrate probe (cpu / fio / iperf3+ping per profile) ─► substrate.json
        │
        ├─ bench suite  GMOUNTIE_BENCH_TCP=1
        │     lan pass (no netem)         ─► bench-lan.txt
        │     wan pass (netem 25ms…)      ─► bench-wan.txt
        │
        ├─ BMF emitter (bench txt + substrate.json) ─► report.bmf.json
        │     measures: latency, throughput, throughput_pct_of_raw
        │
        └─ bencher run --adapter json  ──────────────► Bencher Cloud
                                                         (store, dashboard, thresholds→alert)
   upload bench txt + substrate.json + BMF as CI artifacts
```

## Risks / open considerations

- **Runner upkeep & Pod drift.** The runner Pod must stay registered and node-pinned; a container Pod has extra drift sources versus a VM — node kernel upgrades (FUSE-sensitive), accidental reschedule onto another node, or image rebuilds shifting the toolchain. Mitigation: node affinity + taint hold the Pod in place, toolchain versions are asserted in-job, and the `_substrate/*` benchmarks make floor drift *visible on the dashboard* (with optional thresholds). **Fallback:** if the substrate series shows variance too broad to trust, replace the Pod with a kubevirt VM (registered as a new testbed). Document a "what to check if the dashboard jumps" runbook in `docs/perf/README.md`.
- **Signal density.** Alpha/prod releases are infrequent, so the series is sparse — it's "release-over-release diffs," not a smooth curve. Acceptable per the goal. A future opt-in `workflow_dispatch` for ad-hoc runs could densify it (distinct marker), but is out of scope now.
- **Variance budget.** Even on a fixed, pinned Pod, microbench noise exists. `COUNT`/`BENCHTIME` must be tuned so a real ~5% regression clears the noise floor; thresholds set accordingly. Validate during implementation with a few back-to-back runs of an unchanged commit.
- **BMF schema stability.** We own the BMF emitter, so measure names/units are our contract with Bencher; renaming a benchmark or measure breaks series continuity (Bencher tracks by name). Treat the names as a stable API.
- **Hosted dependency.** Perf data lives on Bencher Cloud. Non-sensitive for public OSS; migration to self-hosted is supported if that changes.

## Validation

- The BMF emitter is unit-tested (testify suite) against captured sample bench output + a sample `substrate.json`, asserting correct measure extraction and normalization math.
- A dry-run of the job on the self-hosted runner against the current `master` tag-equivalent, confirming: FUSE mounts, both profiles run, `netem` RTT assertion passes for `wan`, BMF validates, and `bencher run` lands a visible data point on the dashboard.
- A no-change repeat run to size the variance floor before trusting threshold alerts.
