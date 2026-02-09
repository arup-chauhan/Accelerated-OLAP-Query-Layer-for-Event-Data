#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="benchmarks/results"
mkdir -p "${OUT_DIR}"

EVENT_ID="$(date +%s)"
EVENT_TIME="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
EVENT_EPOCH="$(python3 - <<PY
from datetime import datetime, timezone
print(int(datetime.now(timezone.utc).timestamp()))
PY
)"

curl -sS -X POST "http://localhost:8080/api/ingest/event" \
  -H "Content-Type: application/json" \
  -d "{
    \"type\":\"transaction\",
    \"id\":${EVENT_ID},
    \"userId\":\"freshness-user\",
    \"description\":\"freshness-test\",
    \"amount\":42.0,
    \"currency\":\"USD\",
    \"status\":\"SUCCESS\",
    \"timestamp\":\"${EVENT_TIME}\"
  }" >/dev/null

FOUND_AT=""
for _ in $(seq 1 120); do
  VALUE="$(docker compose exec -T postgres psql -U postgres -d unified -tAc \
    "select metric_value from aggregated_metrics where metric_name='transaction_count' and window_start <= '${EVENT_TIME}'::timestamptz and window_end > '${EVENT_TIME}'::timestamptz limit 1;")"
  if [ -n "${VALUE}" ]; then
    FOUND_AT="$(python3 - <<PY
from datetime import datetime, timezone
print(int(datetime.now(timezone.utc).timestamp()))
PY
)"
    break
  fi
  sleep 1
done

REPORT="${OUT_DIR}/freshness_lag_report.md"
if [ -z "${FOUND_AT}" ]; then
  cat > "${REPORT}" <<EOF
# Freshness Lag Proof

- Event timestamp: ${EVENT_TIME}
- Result: NOT_VISIBLE_WITHIN_120S
EOF
else
  LAG=$((FOUND_AT - EVENT_EPOCH))
  cat > "${REPORT}" <<EOF
# Freshness Lag Proof

- Event timestamp: ${EVENT_TIME}
- Visibility lag: ${LAG}s
EOF
fi

echo "Wrote ${REPORT}"
