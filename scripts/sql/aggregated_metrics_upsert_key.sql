-- Required by Flink JDBC upsert sink:
-- ON CONFLICT (metric_name, window_start, window_end)
ALTER TABLE aggregated_metrics
    ADD CONSTRAINT aggregated_metrics_metric_window_uk
    UNIQUE (metric_name, window_start, window_end);
