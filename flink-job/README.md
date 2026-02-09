# Flink Job Module

This module provides the real-time Flink aggregation path:

- Source: Kafka topics `events.transactions` and `events.activities`
- Processing: event-time tumbling 1-minute windows with bounded out-of-orderness watermarks
- Sink: PostgreSQL `aggregated_metrics` via JDBC upsert

## Run

Build:

```bash
mvn -pl flink-job -am -DskipTests package
```

Run locally:

```bash
flink run -c com.engine.flink.FlinkAggregationJob flink-job/target/flink-job-0.0.1-SNAPSHOT.jar \
  --kafka.bootstrap localhost:9092 \
  --kafka.topic.transactions events.transactions \
  --kafka.topic.activities events.activities \
  --jdbc.url jdbc:postgresql://localhost:5432/unified \
  --jdbc.user postgres \
  --jdbc.password secret \
  --window.allowedLatenessSeconds 30
```

## Important

The upsert sink expects a unique key on `(metric_name, window_start, window_end)` in `aggregated_metrics`.
