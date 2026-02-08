package com.engine.aggregation.grpc;

import com.engine.aggregation.entity.AggregatedMetrics;
import com.engine.aggregation.repository.AggregatedMetricsRepository;
import com.engine.aggregation.service.AggregationService;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.Instant;
import java.util.List;

@GrpcService
public class AggregationServiceImpl extends AggregationServiceGrpc.AggregationServiceImplBase {

    private final AggregatedMetricsRepository repo;
    private final AggregationService aggregationService;

    public AggregationServiceImpl(AggregatedMetricsRepository repo, AggregationService aggregationService) {
        this.repo = repo;
        this.aggregationService = aggregationService;
    }

    @Override
    public void runAggregation(AggregationRequest request, StreamObserver<AggregationResponse> responseObserver) {
        try {
            aggregationService.runJob("/stream/input", "/stream/output",
                    Instant.parse(request.getStartDate()), Instant.parse(request.getEndDate()));

            responseObserver.onNext(AggregationResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getAggregatedMetrics(AggregationRequest request, StreamObserver<AggregatedMetricsDto> responseObserver) {
        List<AggregatedMetrics> results = repo.findByWindowStartGreaterThanEqualAndWindowEndLessThanEqual(
                Instant.parse(request.getStartDate()), Instant.parse(request.getEndDate())
        );

        results.forEach(m ->
                responseObserver.onNext(AggregatedMetricsDto.newBuilder()
                        .setId(m.getId())
                        .setMetricName(m.getMetricName())
                        .setMetricValue(m.getMetricValue())
                        .setWindowStart(m.getWindowStart().toString())
                        .setWindowEnd(m.getWindowEnd().toString())
                        .build())
        );
        responseObserver.onCompleted();
    }
}
