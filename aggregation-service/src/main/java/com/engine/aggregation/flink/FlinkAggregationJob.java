package com.engine.aggregation.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class FlinkAggregationJob {

    private static final Logger log = LoggerFactory.getLogger(FlinkAggregationJob.class);

    public void run(String input, String output) {
        log.info("Triggering Flink aggregation job with input={} output={}", input, output);
    }
}
