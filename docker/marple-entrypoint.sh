#!/usr/bin/env bash
# Wait for OpenSearch, seed the ROR index, then start Marple.
set -euo pipefail

os_url="http://${ES_HOST_DEV:-localhost}:${ES_PORT:-9200}"

if [ -n "${ROR_S3_URI:-}" ]; then
  timeout="${OPENSEARCH_WAIT_TIMEOUT:-300}"
  echo "Waiting for OpenSearch at ${os_url} (timeout ${timeout}s)..."
  curl -sf --retry "$(( timeout / 5 ))" --retry-delay 5 --retry-connrefused --retry-all-errors \
    "${os_url}/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null \
    || { echo "Timed out waiting for OpenSearch at ${os_url}" >&2; exit 1; }
  echo "OpenSearch up; creating index and loading ROR from ${ROR_S3_URI}"
  create-ror-index
  index-ror --s3-path "${ROR_S3_URI}"
  echo "ROR loaded"
fi

echo "Starting Marple"
exec crossref-matcher --host 0.0.0.0 --port 8000
