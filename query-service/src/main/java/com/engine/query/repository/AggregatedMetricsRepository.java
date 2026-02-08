package com.engine.query.repository;

import com.engine.query.entity.AggregatedMetrics;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;

public interface AggregatedMetricsRepository extends JpaRepository<AggregatedMetrics, Long> {
    List<AggregatedMetrics> findByMetricNameAndWindowStartGreaterThanEqualAndWindowEndLessThanEqual(
            String metricName, Instant start, Instant end
    );
}
