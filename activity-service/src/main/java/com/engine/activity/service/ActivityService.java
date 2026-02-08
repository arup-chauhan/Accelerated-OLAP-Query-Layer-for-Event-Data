package com.engine.activity.service;

import com.engine.ingestion.grpc.ActivityDto;
import com.engine.ingestion.grpc.IngestionServiceGrpc;
import com.engine.ingestion.grpc.TransactionDto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Random;

@Service
public class ActivityService {

    private final IngestionServiceGrpc.IngestionServiceBlockingStub stub;
    private final Random random = new Random();

    public ActivityService(
            @Value("${generator.ingestion-host:localhost}") String ingestionHost,
            @Value("${generator.ingestion-port:9090}") int ingestionPort
    ) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(ingestionHost, ingestionPort)
                .usePlaintext()
                .build();
        this.stub = IngestionServiceGrpc.newBlockingStub(channel);
    }

    public void generateTransaction() {
        TransactionDto tx = TransactionDto.newBuilder()
                .setId(random.nextInt(1000000))
                .setDescription("synthetic-tx-" + random.nextInt(1000))
                .setAmount(random.nextDouble() * 1000)
                .setCurrency("USD")
                .setStatus("SUCCESS")
                .setTimestamp(Instant.now().toString())
                .build();
        stub.ingestTransaction(tx);
    }

    public void generateActivity() {
        ActivityDto activity = ActivityDto.newBuilder()
                .setId(random.nextInt(1000000))
                .setActivity(randomAction())
                .setDescription("synthetic-activity-" + random.nextInt(1000))
                .setTimestamp(Instant.now().toString())
                .build();
        stub.ingestActivity(activity);
    }

    private String randomAction() {
        String[] actions = {"LOGIN", "LOGOUT", "PURCHASE", "BROWSE"};
        return actions[random.nextInt(actions.length)];
    }
}
