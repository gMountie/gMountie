#!/usr/bin/env bash
# Apply or clear a named tc netem profile on the loopback interface.
#
# Usage:
#   start-slow-loopback.sh [profile] [iface]
#
# Delegates to scripts/perf/profile.sh apply, which is the single source of
# truth for named netem profiles. Default profile: wan. Default iface: lo.
#
# Profiles: lan (no shaping — clears any existing qdisc), wan (25ms 5ms jitter, 100Mbit).
# Run stop-slow-loopback.sh to remove shaping. Requires root (or sudo).

set -euo pipefail
# Thin wrapper — delegates to scripts/perf/profile.sh.
# Arg contract: [profile] [iface]  (default: wan lo).
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$here/perf/profile.sh" apply "${1:-wan}" "${2:-lo}"
