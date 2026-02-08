package com.engine.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
@RestController
@RequestMapping("/api/ingest")
public class IngestionApp {

    private static final Logger log = LoggerFactory.getLogger(IngestionApp.class);

    public static void main(String[] args) {
        SpringApplication.run(IngestionApp.class, args);
    }

    @PostMapping("/event")
    public String ingestEvent(@RequestBody String payload) {
        // TODO: push to Kafka / Postgres
        log.info("Received event: {}", payload);
        return "Ingested";
    }
}
