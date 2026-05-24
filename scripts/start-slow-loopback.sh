#!/usr/bin/env bash
# Apply tc netem shaping to the loopback interface for realistic perf testing.
#
# Usage:
#   start-slow-loopback.sh [profile] [iface]
#
# Delegates to scripts/perf/profile.sh apply, which is the single source of
# truth for named netem profiles. Default profile: wan. Default iface: lo.
#
# Profiles: lan (no shaping), wan (25ms 5ms jitter, 100Mbit).
# Run stop-slow-loopback.sh to remove shaping. Requires root (or sudo).

set -euo pipefail
# Back-compat wrapper: the canonical profile definitions now live in
# scripts/perf/profile.sh. Default behaviour applies the WAN profile to lo.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$here/perf/profile.sh" apply "${1:-wan}" "${2:-lo}"
