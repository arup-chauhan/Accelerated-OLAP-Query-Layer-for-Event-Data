package com.engine.query.service;

import com.engine.cache.grpc.CacheEntry;
import com.engine.cache.grpc.CacheRequest;
import com.engine.cache.grpc.CacheResponse;
import com.engine.cache.grpc.CacheServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CacheClient {
    private final CacheServiceGrpc.CacheServiceBlockingStub stub;

    public CacheClient(
            @Value("${cache.grpc.host:localhost}") String host,
            @Value("${cache.grpc.port:9090}") int port
    ) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.stub = CacheServiceGrpc.newBlockingStub(channel);
    }

    public String get(String key) {
        CacheResponse response = stub.get(CacheRequest.newBuilder().setKey(key).build());
        return response.getValue();
    }

    public void set(String key, String value) {
        stub.set(CacheEntry.newBuilder().setKey(key).setValue(value).build());
    }
}
