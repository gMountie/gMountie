#!/usr/bin/env bash
# Remove any tc netem shaping on the loopback interface (or a named interface).
# Delegates to scripts/perf/profile.sh, the single source of truth for perf
# network profiles. Optional arg: [iface] (default: lo). Requires root (or sudo).

set -euo pipefail
# Thin wrapper — delegates to scripts/perf/profile.sh, the single source of
# truth for perf network profiles. Clears any netem qdisc on the interface.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$here/perf/profile.sh" clear "${1:-lo}"
