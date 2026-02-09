package com.engine.common.kafka;

public final class KafkaTopics {

    public static final String TRANSACTIONS = "events.transactions";
    public static final String ACTIVITIES = "events.activities";
    public static final String TRANSACTIONS_DLQ = "events.transactions.dlq";
    public static final String ACTIVITIES_DLQ = "events.activities.dlq";

    private KafkaTopics() {
    }
}
