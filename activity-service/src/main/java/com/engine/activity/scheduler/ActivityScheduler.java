package com.engine.activity.scheduler;

import com.engine.activity.service.ActivityService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ActivityScheduler {

    private final ActivityService activityService;

    public ActivityScheduler(ActivityService activityService) {
        this.activityService = activityService;
    }

    // Generate a transaction every 2 seconds
    @Scheduled(fixedRate = 2000)
    public void generateTransactionLog() {
        activityService.generateTransaction();
    }

    // Generate an activity every 3 seconds
    @Scheduled(fixedRate = 3000)
    public void generateActivityLog() {
        activityService.generateActivity();
    }
}
