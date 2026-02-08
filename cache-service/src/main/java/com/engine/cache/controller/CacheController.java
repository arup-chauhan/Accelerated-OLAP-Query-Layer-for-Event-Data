package com.engine.cache.controller;

import com.engine.cache.service.RedisCacheService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/cache")
public class CacheController {

    private final RedisCacheService cache;

    public CacheController(RedisCacheService cache) {
        this.cache = cache;
    }

    @DeleteMapping("/invalidate/{key}")
    public String invalidate(@PathVariable String key) {
        cache.invalidate(key);
        return "Invalidated key: " + key;
    }
}
