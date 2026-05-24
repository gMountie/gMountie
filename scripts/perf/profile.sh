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
# value. Requires root (privileged Pod / sudo).
set -euo pipefail

WAN_NETEM="delay 25ms 5ms rate 100Mbit"

cmd="${1:-}"
case "$cmd" in
  apply)
    profile="${2:-}"
    iface="${3:-lo}"
    case "$profile" in
      lan) tc qdisc del dev "$iface" root 2>/dev/null || true ;;
      wan)
        # shellcheck disable=SC2086 # WAN_NETEM is an intentional arg list
        tc qdisc replace dev "$iface" root netem $WAN_NETEM 2>/dev/null \
          || tc qdisc add dev "$iface" root netem $WAN_NETEM ;;
      *) echo "profile.sh: unknown profile '$profile' (want lan|wan)" >&2; exit 2 ;;
    esac
    tc qdisc show dev "$iface" ;;
  clear)
    iface="${2:-lo}"
    tc qdisc del dev "$iface" root 2>/dev/null || true
    echo "cleared netem on $iface" ;;
  *)
    echo "usage: profile.sh apply <lan|wan> [iface] | clear [iface]" >&2
    exit 2 ;;
esac
