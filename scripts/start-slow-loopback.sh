#!/usr/bin/env bash
# Apply tc netem shaping to the loopback interface for realistic perf testing.
#
# Usage:
#   start-slow-loopback.sh [delay] [rate]
#
# Defaults: 30ms delay, 1000Mbit rate cap. Both args accept any tc-recognised
# spec (e.g. "30ms", "100Mbit", "0" to omit a knob — pass empty string to skip).
#
# The script is idempotent: if a netem qdisc already exists on lo it is
# replaced via `tc qdisc change`; otherwise `tc qdisc add` installs a fresh
# one. Run stop-slow-loopback.sh to remove it. Requires root (or sudo).

set -euo pipefail

DELAY="${1:-30ms}"
RATE="${2:-1000Mbit}"

netem_args=()
if [ -n "$DELAY" ]; then
  netem_args+=(delay "$DELAY")
fi
if [ -n "$RATE" ]; then
  netem_args+=(rate "$RATE")
fi

if [ ${#netem_args[@]} -eq 0 ]; then
  echo "start-slow-loopback: nothing to do (no delay, no rate)" >&2
  exit 1
fi

# `tc qdisc replace` is the idempotent form — installs if absent, changes if
# present. Falls back to `add` on older tc that doesn't support `replace`.
if ! tc qdisc replace dev lo root netem "${netem_args[@]}" 2>/dev/null; then
  tc qdisc add dev lo root netem "${netem_args[@]}"
fi

echo "loopback shaping active:"
tc qdisc show dev lo
