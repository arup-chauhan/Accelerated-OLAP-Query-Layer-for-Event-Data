CREATE TABLE IF NOT EXISTS aggregated_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'aggregated_metrics_metric_window_uk'
    ) THEN
        ALTER TABLE aggregated_metrics
            ADD CONSTRAINT aggregated_metrics_metric_window_uk
            UNIQUE (metric_name, window_start, window_end);
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_metric_window
    ON aggregated_metrics (metric_name, window_start, window_end);
