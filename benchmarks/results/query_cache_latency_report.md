# Query Cache Latency Proof

- Endpoint: `http://localhost:8082/api/query/transactions/benchmark-user?start=2026-02-09T09:28:25Z&end=2026-02-09T10:28:25Z`
- Samples:
  - uncached (first call): 0.112534s
  - cached sample count: 25
  - cached p50: 0.005762s
  - cached p95: 0.008598s
