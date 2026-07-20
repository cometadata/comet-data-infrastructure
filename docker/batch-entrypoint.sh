#!/usr/bin/env bash
# When WAIT_FOR_ROR_READY=1, wait for Marple to be ready before running the command.
set -euo pipefail

if [ "${WAIT_FOR_ROR_READY:-}" = "1" ]; then
  health_url="http://localhost:8000/health"
  timeout="${ROR_READY_TIMEOUT:-1800}"
  echo "Waiting for Marple readiness at ${health_url} (timeout ${timeout}s)..."
  curl -sf --retry "$(( timeout / 5 ))" --retry-delay 5 --retry-connrefused --retry-all-errors \
    "${health_url}" >/dev/null \
    || { echo "Timed out waiting for Marple readiness at ${health_url}" >&2; exit 1; }
  echo "Ready: Marple ${health_url} returned 200"
fi

exec "$@"
