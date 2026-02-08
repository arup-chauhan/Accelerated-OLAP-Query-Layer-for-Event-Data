package com.engine.aggregation.repository;

import com.engine.aggregation.entity.AggregatedMetrics;
import org.springframework.data.jpa.repository.JpaRepository;
import java.time.Instant;
import java.util.List;

public interface AggregatedMetricsRepository extends JpaRepository<AggregatedMetrics, Long> {
    List<AggregatedMetrics> findByWindowStartGreaterThanEqualAndWindowEndLessThanEqual(
            Instant start, Instant end
    );
}
