#!/usr/bin/env bash
# Apply or clear a named perf network profile via tc netem.
#
# Usage:
#   profile.sh apply <lan|wan> [iface]   # default iface: lo
#   profile.sh clear [iface]
#
# Profiles (the single source of truth for LAN/WAN shaping):
#   lan  -> no shaping (qdisc cleared)
#   wan  -> netem delay 25ms 5ms rate 100Mbit
#
# Note: on loopback a packet traverses the qdisc once per direction, so a
# 25ms delay yields ~50ms RTT. The run harness asserts against that doubled
# value. tc mutations need root; this script auto-elevates via sudo when not
# already root (the perf runner runs as a non-root user in a privileged Pod).
set -euo pipefail

WAN_NETEM="delay 25ms 5ms rate 100Mbit"

# run_tc runs tc as root. A privileged Pod grants the *container* caps, but a
# non-root process's effective caps are still empty, so tc needs sudo unless we
# are already root. (sudo for tc is baked into the perf runner image.)
run_tc() {
  if [ "$(id -u)" -eq 0 ]; then tc "$@"; else sudo tc "$@"; fi
}

cmd="${1:-}"
case "$cmd" in
  apply)
    profile="${2:-}"
    iface="${3:-lo}"
    case "$profile" in
      lan) run_tc qdisc del dev "$iface" root 2>/dev/null || true ;;
      wan)
        # shellcheck disable=SC2086 # WAN_NETEM is an intentional arg list
        run_tc qdisc replace dev "$iface" root netem $WAN_NETEM 2>/dev/null \
          || run_tc qdisc add dev "$iface" root netem $WAN_NETEM ;;
      *) echo "profile.sh: unknown profile '$profile' (want lan|wan)" >&2; exit 2 ;;
    esac
    tc qdisc show dev "$iface" ;;
  clear)
    iface="${2:-lo}"
    run_tc qdisc del dev "$iface" root 2>/dev/null || true
    echo "cleared netem on $iface" ;;
  *)
    echo "usage: profile.sh apply <lan|wan> [iface] | clear [iface]" >&2
    exit 2 ;;
esac
