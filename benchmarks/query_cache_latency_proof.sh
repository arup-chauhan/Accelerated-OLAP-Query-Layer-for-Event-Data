#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="benchmarks/results"
mkdir -p "${OUT_DIR}"

WINDOW_END="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
WINDOW_START="$(date -u -v-1H +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || python3 - <<'PY'
from datetime import datetime, timedelta, timezone
print((datetime.now(timezone.utc)-timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)"

USER_ID="benchmark-user"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-accelerated-olap-query-layer-event-data}"
CONTAINER_NAME="${PROJECT_NAME}-query-service-1"
HOST_URL="http://localhost:8082/api/query/transactions/${USER_ID}?start=${WINDOW_START}&end=${WINDOW_END}"
CONTAINER_URL="http://localhost:8080/api/query/transactions/${USER_ID}?start=${WINDOW_START}&end=${WINDOW_END}"
URL="${HOST_URL}"

if MISS_TIME="$(curl -sS -o /dev/null -w '%{time_total}' "${HOST_URL}" 2>/dev/null)"; then
  SAMPLE_CMD=(curl -sS -o /dev/null -w '%{time_total}\n' "${HOST_URL}")
else
  URL="${CONTAINER_URL}"
  MISS_TIME="$(docker exec "${CONTAINER_NAME}" curl -sS -o /dev/null -w '%{time_total}' "${CONTAINER_URL}")"
  SAMPLE_CMD=(docker exec "${CONTAINER_NAME}" curl -sS -o /dev/null -w '%{time_total}\n' "${CONTAINER_URL}")
fi

HIT_SAMPLES_FILE="${OUT_DIR}/query_cache_hit_samples.txt"
rm -f "${HIT_SAMPLES_FILE}"
for _ in $(seq 1 25); do
  "${SAMPLE_CMD[@]}" >> "${HIT_SAMPLES_FILE}"
done

P50="$(sort -n "${HIT_SAMPLES_FILE}" | awk 'NR==13{print $1}')"
P95="$(sort -n "${HIT_SAMPLES_FILE}" | awk 'NR==24{print $1}')"

REPORT="${OUT_DIR}/query_cache_latency_report.md"
cat > "${REPORT}" <<EOF
# Query Cache Latency Proof

- Endpoint: \`${URL}\`
- Samples:
  - uncached (first call): ${MISS_TIME}s
  - cached sample count: 25
  - cached p50: ${P50}s
  - cached p95: ${P95}s
EOF

echo "Wrote ${REPORT}"
