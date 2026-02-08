package com.engine.query.grpc;

import com.engine.query.entity.AggregatedMetrics;
import com.engine.query.repository.AggregatedMetricsRepository;
import com.engine.query.service.CacheClient;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.Instant;
import java.util.List;

@GrpcService
public class QueryServiceImpl extends QueryServiceGrpc.QueryServiceImplBase {

    private final AggregatedMetricsRepository repo;
    private final CacheClient cache;

    public QueryServiceImpl(AggregatedMetricsRepository repo, CacheClient cache) {
        this.repo = repo;
        this.cache = cache;
    }

    @Override
    public void getAggregatedMetrics(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        String cacheKey = request.getMetricName() + ":" + request.getStartDate() + ":" + request.getEndDate();
        try {
            // 1. Try cache
            String cached = cache.get(cacheKey);
            if (cached != null && !cached.isEmpty()) {
                // Cache holds JSON or CSV, here assume CSV-like "metricValue"
                QueryResponse resp = QueryResponse.newBuilder()
                        .setId(0) // no DB id for cached entry
                        .setMetricName(request.getMetricName())
                        .setMetricValue(Double.parseDouble(cached))
                        .setWindowStart(request.getStartDate())
                        .setWindowEnd(request.getEndDate())
                        .build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
                return;
            }

            // 2. Fallback to DB
            Instant start = Instant.parse(request.getStartDate());
            Instant end = Instant.parse(request.getEndDate());
            List<AggregatedMetrics> results =
                    repo.findByMetricNameAndWindowStartGreaterThanEqualAndWindowEndLessThanEqual(
                            request.getMetricName(), start, end);

            for (AggregatedMetrics m : results) {
                QueryResponse resp = QueryResponse.newBuilder()
                        .setId(m.getId())
                        .setMetricName(m.getMetricName())
                        .setMetricValue(m.getMetricValue())
                        .setWindowStart(m.getWindowStart().toString())
                        .setWindowEnd(m.getWindowEnd().toString())
                        .build();
                responseObserver.onNext(resp);
            }

            // 3. Repopulate cache (simple: first result)
            if (!results.isEmpty()) {
                cache.set(cacheKey, String.valueOf(results.get(0).getMetricValue()));
            }

            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
