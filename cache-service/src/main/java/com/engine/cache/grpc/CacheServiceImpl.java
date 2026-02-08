package com.engine.cache.grpc;

import com.engine.cache.service.RedisCacheService;
import com.engine.cache.grpc.CacheAck;
import com.engine.cache.grpc.CacheEntry;
import com.engine.cache.grpc.CacheRequest;
import com.engine.cache.grpc.CacheResponse;
import com.engine.cache.grpc.CacheServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.Duration;

@GrpcService
public class CacheServiceImpl extends CacheServiceGrpc.CacheServiceImplBase {

    private final RedisCacheService cache;

    public CacheServiceImpl(RedisCacheService cache) {
        this.cache = cache;
    }

    @Override
    public void get(CacheRequest request, StreamObserver<CacheResponse> responseObserver) {
        String value = cache.get(request.getKey());
        responseObserver.onNext(CacheResponse.newBuilder()
                .setValue(value == null ? "" : value)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void set(CacheEntry request, StreamObserver<CacheAck> responseObserver) {
        cache.set(request.getKey(), request.getValue(), Duration.ofSeconds(60));
        responseObserver.onNext(CacheAck.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void invalidate(CacheRequest request, StreamObserver<CacheAck> responseObserver) {
        cache.invalidate(request.getKey());
        responseObserver.onNext(CacheAck.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }
}
