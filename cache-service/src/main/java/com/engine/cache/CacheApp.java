package com.engine.cache;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;


@SpringBootApplication
@RestController
@RequestMapping("/api/cache")
public class CacheApp {

    private static final Logger log = LoggerFactory.getLogger(CacheApp.class);

    @Autowired
    private StringRedisTemplate redisTemplate;

    public static void main(String[] args) {
        SpringApplication.run(CacheApp.class, args);
    }

    @GetMapping("/get/{key}")
    public String get(@PathVariable String key) {
        String value = redisTemplate.opsForValue().get(key);
        log.info("Cache get {} -> {}", key, value);
        return value != null ? value : "Not found";
    }

    @PostMapping("/set/{key}")
    public String set(@PathVariable String key, @RequestBody String value) {
        redisTemplate.opsForValue().set(key, value);
        log.info("Cache set {} -> {}", key, value);
        return "OK";
    }
}
