package com.engine.ingestion.kafka;

import com.engine.ingestion.grpc.ActivityDto;
import com.engine.ingestion.grpc.TransactionDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class IngestionEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(IngestionEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final boolean enabled;
    private final String transactionTopic;
    private final String activityTopic;

    public IngestionEventPublisher(
            ObjectProvider<KafkaTemplate<String, String>> kafkaTemplateProvider,
            ObjectMapper objectMapper,
            @Value("${engine.kafka.enabled:false}") boolean enabled,
            @Value("${engine.kafka.topic.transactions:events.transactions}") String transactionTopic,
            @Value("${engine.kafka.topic.activities:events.activities}") String activityTopic
    ) {
        this.kafkaTemplate = kafkaTemplateProvider.getIfAvailable();
        this.objectMapper = objectMapper;
        this.enabled = enabled;
        this.transactionTopic = transactionTopic;
        this.activityTopic = activityTopic;
    }

    public void publishTransaction(TransactionDto dto) {
        Map<String, Object> event = new HashMap<>();
        event.put("id", dto.getId());
        event.put("description", dto.getDescription());
        event.put("amount", dto.getAmount());
        event.put("currency", dto.getCurrency());
        event.put("status", dto.getStatus());
        event.put("timestamp", dto.getTimestamp());
        publish(transactionTopic, String.valueOf(dto.getId()), event);
    }

    public void publishActivity(ActivityDto dto) {
        Map<String, Object> event = new HashMap<>();
        event.put("id", dto.getId());
        event.put("activity", dto.getActivity());
        event.put("description", dto.getDescription());
        event.put("timestamp", dto.getTimestamp());
        publish(activityTopic, String.valueOf(dto.getId()), event);
    }

    private void publish(String topic, String key, Map<String, Object> event) {
        if (!enabled) {
            return;
        }
        if (kafkaTemplate == null) {
            log.warn("Kafka publishing enabled but KafkaTemplate is not configured");
            return;
        }
        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topic, key, payload);
        } catch (Exception e) {
            log.error("Failed to publish event to Kafka topic {}", topic, e);
        }
    }
}
