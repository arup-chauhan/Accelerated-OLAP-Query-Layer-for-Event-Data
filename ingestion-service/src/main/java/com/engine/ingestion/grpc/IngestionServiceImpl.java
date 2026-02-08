package com.engine.ingestion.grpc;

import com.engine.ingestion.service.IngestionService;
import com.engine.ingestion.grpc.ActivityDto;
import com.engine.ingestion.grpc.TransactionDto;
import com.engine.ingestion.grpc.IngestResponse;
import com.engine.ingestion.grpc.IngestionServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class IngestionServiceImpl extends IngestionServiceGrpc.IngestionServiceImplBase {

    private final IngestionService ingestionService;

    public IngestionServiceImpl(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @Override
    public void ingestTransaction(TransactionDto request, StreamObserver<IngestResponse> responseObserver) {
        ingestionService.saveTransaction(request);
        responseObserver.onNext(IngestResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void ingestActivity(ActivityDto request, StreamObserver<IngestResponse> responseObserver) {
        ingestionService.saveActivity(request);
        responseObserver.onNext(IngestResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }
}
