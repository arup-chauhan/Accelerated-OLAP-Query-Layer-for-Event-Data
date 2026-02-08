package com.engine.query;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
@RestController
@RequestMapping("/api/query")
public class QueryApp {

    private static final Logger log = LoggerFactory.getLogger(QueryApp.class);

    public static void main(String[] args) {
        SpringApplication.run(QueryApp.class, args);
    }

    @GetMapping("/transactions/{userId}")
    public String getTransactions(@PathVariable String userId) {
        // TODO: query Postgres + Redis cache
        log.info("Fetching transactions for user {}", userId);
        return "Transaction summary for " + userId;
    }

    @GetMapping("/activities/{userId}")
    public String getActivities(@PathVariable String userId) {
        // TODO: query activity trends
        log.info("Fetching activity trends for user {}", userId);
        return "Activity trends for " + userId;
    }
}
