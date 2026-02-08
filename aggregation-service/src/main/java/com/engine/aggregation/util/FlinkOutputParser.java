package com.engine.aggregation.util;

import com.engine.aggregation.entity.AggregatedMetrics;
import com.engine.aggregation.repository.AggregatedMetricsRepository;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;
import java.time.Instant;

@Component
public class FlinkOutputParser {

    private final AggregatedMetricsRepository repo;

    public FlinkOutputParser(AggregatedMetricsRepository repo) {
        this.repo = repo;
    }

    public void parseAndPersist(String outputPath, Instant windowStart, Instant windowEnd) {
        String file = Paths.get(outputPath, "part-r-00000").toString();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Expected format: key \t value
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    AggregatedMetrics metric = new AggregatedMetrics();
                    metric.setMetricName(parts[0]);
                    metric.setMetricValue(Double.parseDouble(parts[1]));
                    metric.setWindowStart(windowStart);
                    metric.setWindowEnd(windowEnd);
                    repo.save(metric);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Flink output", e);
        }
    }
}
