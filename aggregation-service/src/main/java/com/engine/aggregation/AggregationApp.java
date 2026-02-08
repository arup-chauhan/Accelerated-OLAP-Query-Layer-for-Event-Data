package com.engine.aggregation;

import com.engine.aggregation.service.AggregationService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

@SpringBootApplication
@RestController
@RequestMapping("/api/aggregate")
public class AggregationApp {

    private static final Logger log = LoggerFactory.getLogger(AggregationApp.class);

    private final AggregationService aggregationService;

    @Autowired
    public AggregationApp(AggregationService aggregationService) {
        this.aggregationService = aggregationService;
    }

    public static void main(String[] args) {
        SpringApplication.run(AggregationApp.class, args);
    }

    @PostMapping("/run")
    public String runAggregation(
            @RequestParam(defaultValue = "/stream/input") String inputPath,
            @RequestParam(defaultValue = "/stream/output") String outputPath,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate) {

        log.info("Starting Flink aggregation run: input={}, output={}, start={}, end={}",
                inputPath, outputPath, startDate, endDate);

        try {
            if (startDate != null && endDate != null) {
                aggregationService.runJob(inputPath, outputPath,
                        Instant.parse(startDate), Instant.parse(endDate));
            } else {
                aggregationService.runJob(inputPath, outputPath);
            }
            return "Aggregation job submitted successfully.";
        } catch (Exception e) {
            log.error("Failed to run aggregation job", e);
            return "Error: " + e.getMessage();
        }
    }

    @GetMapping("/status")
    public String checkStatus() {
        return aggregationService.getStatus();
    }
}
