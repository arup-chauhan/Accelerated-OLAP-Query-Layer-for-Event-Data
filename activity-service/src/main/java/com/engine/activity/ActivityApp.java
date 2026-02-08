package com.engine.activity;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ActivityApp {
    public static void main(String[] args) {
        SpringApplication.run(ActivityApp.class, args);
    }
}
