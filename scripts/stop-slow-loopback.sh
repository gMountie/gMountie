#!/usr/bin/env bash
# Remove any tc netem shaping previously applied to the loopback interface.
# Idempotent: no-op if no qdisc is currently installed. Requires root (or sudo).

set -euo pipefail

if tc qdisc show dev lo | grep -q '^qdisc netem'; then
  tc qdisc del dev lo root
  echo "loopback shaping removed"
else
  echo "no netem qdisc on lo — nothing to do"
fi
