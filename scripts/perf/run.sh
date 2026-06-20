#!/usr/bin/env bash
# Orchestrate one perf run: substrate probe -> benches over lan+wan ->
# BMF -> (optional) bencher upload. Designed to run on the self-hosted
# perf runner. Honours these env vars:
#
#   COUNT        go test -bench -count        (default 10)
#   BENCHTIME    go test -benchtime           (default 10s)
#   WORKDIR      scratch dir; MUST be on the local PV (default ./perf-out)
#   SUBSTRATE_DIR  fio probe dir (default $TMPDIR == bench data dir)
#   IFACE        interface to shape           (default lo)
#   EXPECT_GO_VERSION / EXPECT_FIO_VERSION / EXPECT_IPERF3_VERSION /
#   EXPECT_BENCHER_VERSION  optional pinned versions; mismatch fails the run
#   BENCHER      "1" to run bencher upload    (default unset = skip)
#   BENCHER_PROJECT / BENCHER_TESTBED / BENCHER_BRANCH / GIT_HASH
#   FUSE_BINDING gofuse|cgofuse  select the FUSE adapter for go test
#                (default: gofuse — pure Go, CGO_ENABLED=0)
#                cgofuse requires libfuse-dev; sets CGO_ENABLED=1 -tags cgofuse.
#                See docs/design/benchmarks/cgofuse-vs-gofuse.md.
#
# Requires: go, fio, iperf3, ping, tc, ss, and a built perfbmf at $PERFBMF
# (default: built into $WORKDIR).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

COUNT="${COUNT:-10}"
BENCHTIME="${BENCHTIME:-10s}"
WORKDIR="${WORKDIR:-$repo/perf-out}"
# The bench harness creates its data dir via os.MkdirTemp("", ...), which
# honours $TMPDIR. Point TMPDIR and the fio probe at the SAME directory so the
# substrate disk number reflects the filesystem the benches actually hit.
# WORKDIR must live on the runner's local PV — not the ephemeral overlay, and
# not tmpfs (fio direct=1 needs a real block-backed fs).
export TMPDIR="$WORKDIR/data"
export SUBSTRATE_DIR="${SUBSTRATE_DIR:-$TMPDIR}"
IFACE="${IFACE:-lo}"
PERFBMF="${PERFBMF:-$WORKDIR/perfbmf}"
# FUSE_BINDING selects the FUSE adapter compiled into the go test binary.
# Default is gofuse (pure Go, no cgo). Set to "cgofuse" to compile with
# CGO_ENABLED=1 and -tags cgofuse for the comparison run.
FUSE_BINDING="${FUSE_BINDING:-gofuse}"
case "$FUSE_BINDING" in
  gofuse)  _CGO_ENABLED=0; _TAGS="";;
  cgofuse) _CGO_ENABLED=1; _TAGS="-tags cgofuse";;
  *) echo "FUSE_BINDING must be gofuse or cgofuse (got '$FUSE_BINDING')" >&2; exit 1;;
esac

mkdir -p "$WORKDIR" "$TMPDIR" "$SUBSTRATE_DIR"

# Go commands below use module-relative ./test/... paths; anchor at the repo.
cd "$repo"

# Always leave the interface unshaped and kill any leaked iperf3 server on
# exit, even on failure (a failed client can orphan the one-shot -s -1 -D).
cleanup() {
  pkill -x iperf3 2>/dev/null || true
  "$here/profile.sh" clear "$IFACE" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== assert toolchain present =="
for bin in go fio iperf3 ping tc ss; do command -v "$bin" >/dev/null || { echo "missing $bin" >&2; exit 1; }; done
[ "${BENCHER:-}" = "1" ] && { command -v bencher >/dev/null || { echo "missing bencher" >&2; exit 1; }; }

echo "== assert pinned tool versions =="
# Pins live with the runner image (infra repo) and arrive via EXPECT_* env.
# When set, a drifted tool fails the run loudly instead of silently moving
# the substrate floor — the spec's runner-upkeep mitigation, enforced here.
assert_version() { # $1=label $2=actual $3=expected(optional)
  echo "$1: $2"
  if [ -n "${3:-}" ] && [ "$2" != "$3" ]; then echo "  ERROR: expected '$3'" >&2; exit 1; fi
}
assert_version go      "$(go version | awk '{print $3}')"                "${EXPECT_GO_VERSION:-}"
assert_version fio     "$(fio --version)"                                "${EXPECT_FIO_VERSION:-}"
assert_version iperf3  "$(iperf3 --version | head -1 | awk '{print $2}')" "${EXPECT_IPERF3_VERSION:-}"
[ "${BENCHER:-}" = "1" ] && assert_version bencher "$(bencher --version | awk '{print $NF}')" "${EXPECT_BENCHER_VERSION:-}"

echo "== build perfbmf =="
go build -o "$PERFBMF" ./test/e2e/perf/cmd/perfbmf/

echo "== cpu + disk substrate =="
cpu="$("$PERFBMF" cpuprobe)"
fio --output-format=json "$repo/test/e2e/perf/substrate/substrate.fio" > "$WORKDIR/fio.json"

# Wait until something is LISTENing on TCP :5201. A fixed sleep between
# server start and client connect was a flake source under load (set -e
# kills the whole run on one refused connect).
wait_iperf3_listening() {
  local i
  for i in $(seq 1 100); do
    if ss -ltn 2>/dev/null | grep -q ':5201[[:space:]]'; then return 0; fi
    sleep 0.1
  done
  echo "iperf3 server not listening on :5201 after 10s" >&2
  return 1
}

run_net_probe() { # $1=profile $2=iperf seconds
  local p="$1" secs="$2"
  "$here/profile.sh" apply "$p" "$IFACE" >/dev/null
  iperf3 -s -1 -D                                  # one-shot server, daemonized
  wait_iperf3_listening
  # WAN needs a longer window so TCP slow-start doesn't drag the average
  # below the 100 Mbit cap; LAN over lo settles almost instantly.
  iperf3 -c 127.0.0.1 -t "$secs" -J > "$WORKDIR/iperf-$p.json"
  ping -c 20 -i 0.1 127.0.0.1 > "$WORKDIR/ping-$p.txt"
}

run_bench() { # $1=profile
  local p="$1"
  # CGO_ENABLED and build tags come from FUSE_BINDING (gofuse default, cgofuse opt-in).
  GMOUNTIE_BENCH_TCP=1 CGO_ENABLED="$_CGO_ENABLED" go test -run=^$ -bench=. -benchmem \
    -count="$COUNT" -benchtime="$BENCHTIME" ${_TAGS} ./test/e2e/perf/ \
    | tee "$WORKDIR/bench-$p.txt"
}

assert_rtt() { # $1=profile $2=min $3=max
  local rtt; rtt="$(awk -F/ '/rtt/{print $5}' "$WORKDIR/ping-$1.txt")"
  awk -v r="$rtt" -v lo="$2" -v hi="$3" 'BEGIN{exit !(r>=lo && r<=hi)}' \
    || { echo "profile $1 RTT ${rtt}ms outside [$2,$3] — netem not applied as expected" >&2; exit 1; }
}

for p in lan wan; do
  echo "== profile $p: net probe =="
  if [ "$p" = wan ]; then run_net_probe "$p" 8; else run_net_probe "$p" 3; fi
  echo "== profile $p: benches =="
  run_bench "$p"
done
"$here/profile.sh" clear "$IFACE" >/dev/null

# LAN loopback RTT is sub-millisecond; WAN delay 25ms -> ~50ms RTT round trip.
assert_rtt lan 0 5
assert_rtt wan 40 60

echo "== assemble substrate.json =="
"$PERFBMF" substrate --cpu "$cpu" --fio "$WORKDIR/fio.json" \
  --iperf-lan "$WORKDIR/iperf-lan.json" --ping-lan "$WORKDIR/ping-lan.txt" \
  --iperf-wan "$WORKDIR/iperf-wan.json" --ping-wan "$WORKDIR/ping-wan.txt" \
  --out "$WORKDIR/substrate.json"

echo "== emit BMF =="
"$PERFBMF" emit --substrate "$WORKDIR/substrate.json" \
  --bench-lan "$WORKDIR/bench-lan.txt" --bench-wan "$WORKDIR/bench-wan.txt" \
  --out "$WORKDIR/report.bmf.json"

if [ "${BENCHER:-}" = "1" ]; then
  echo "== bencher run =="
  bencher run \
    --project "$BENCHER_PROJECT" \
    --branch "${BENCHER_BRANCH:-master}" \
    --testbed "$BENCHER_TESTBED" \
    --hash "${GIT_HASH:-$(git rev-parse HEAD)}" \
    --adapter json \
    --file "$WORKDIR/report.bmf.json"
else
  echo "BENCHER!=1, skipping upload; report at $WORKDIR/report.bmf.json"
fi
