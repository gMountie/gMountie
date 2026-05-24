# Custom ARC runner image for the gMountie release-gated perf pipeline.
#
# Extends the official GitHub Actions runner image (the base gha-runner-scale-set
# uses) with the toolchain scripts/perf/run.sh invokes, plus passwordless sudo
# for `tc`. The runner stays the non-root `runner` user (uid 1001); the Pod is
# privileged so /dev/fuse is usable and fusermount3 (setuid) lets the unprivileged
# runner mount FUSE, while `tc` (netem) elevates via sudo.
#
# Built and pushed manually (not via CI):
#   docker build -t ghcr.io/gmountie/gmountie-perf-runner:0.1.0 deployments/perf-runner
#   docker push   ghcr.io/gmountie/gmountie-perf-runner:0.1.0
#
# Pin every ARG below — the image version is part of the substrate identity, and
# run.sh's EXPECT_* guards can assert these exact versions at job time.
FROM ghcr.io/actions/actions-runner:2.319.1

USER root

# Perf toolchain:
#   fio            disk substrate probe (direct=1)
#   iperf3, iputils-ping  network substrate probe
#   iproute2       provides `tc` for the WAN netem profile
#   fuse3          fusermount3 (setuid) for the unprivileged FUSE mount + libfuse
#   sudo           tc needs root even in a privileged Pod (uid 1001 has no caps)
#   git, curl, ca-certificates  checkout + downloads + HTTPS to bencher.dev
RUN apt-get update && apt-get install -y --no-install-recommends \
        fio \
        iperf3 \
        iputils-ping \
        iproute2 \
        fuse3 \
        sudo \
        git \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Passwordless sudo scoped to tc for the runner user (WAN netem only). Both
# common tc paths are allowed so the entry survives base-image path differences.
RUN printf 'runner ALL=(root) NOPASSWD: /usr/sbin/tc, /sbin/tc\n' \
        > /etc/sudoers.d/perf-tc \
    && chmod 0440 /etc/sudoers.d/perf-tc

# Match the gMountie FUSE setup (allow_other mounts).
RUN echo user_allow_other >> /etc/fuse.conf

# Go toolchain, pinned to go.mod's version.
ARG GO_VERSION=1.26.3
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
        | tar -C /usr/local -xz
ENV PATH=/usr/local/go/bin:/home/runner/go/bin:${PATH}

# benchstat — parity with `task perf:diff` for local diffing on the runner.
ARG BENCHSTAT_VERSION=latest
RUN GOBIN=/usr/local/bin GOFLAGS=-mod=mod \
        go install "golang.org/x/perf/cmd/benchstat@${BENCHSTAT_VERSION}"

# Bencher CLI — the upload step (`bencher run`). Pin BENCHER_VERSION; confirm the
# install mechanism/asset at https://bencher.dev/docs/how-to/install-cli/.
ARG BENCHER_VERSION=0.5.0
RUN curl -fsSL "https://bencher.dev/download/install-cli.sh" \
        | VERSION="${BENCHER_VERSION}" sh \
    && bencher --version

# Back to the unprivileged runner user the chart launches as.
USER runner
