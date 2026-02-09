package com.engine.flink;

import com.engine.common.kafka.KafkaTopics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;

public class FlinkAggregationJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String kafkaBootstrap = params.get("kafka.bootstrap", "localhost:9092");
        String txTopic = params.get("kafka.topic.transactions", KafkaTopics.TRANSACTIONS);
        String activityTopic = params.get("kafka.topic.activities", KafkaTopics.ACTIVITIES);
        String groupId = params.get("kafka.group.id", "flink-aggregation-job");
        String jdbcUrl = params.get("jdbc.url", "jdbc:postgresql://localhost:5432/unified");
        String jdbcUser = params.get("jdbc.user", "postgres");
        String jdbcPassword = params.get("jdbc.password", "secret");
        int allowedLatenessSeconds = params.getInt("window.allowedLatenessSeconds", 30);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStream<MetricDelta> txDeltas = env
                .fromSource(kafkaSource(kafkaBootstrap, txTopic, groupId), WatermarkStrategy.noWatermarks(), "tx-source")
                .flatMap((String raw, Collector<MetricDelta> out) -> emitTransactionDeltas(raw, out))
                .returns(MetricDelta.class);

        DataStream<MetricDelta> activityDeltas = env
                .fromSource(kafkaSource(kafkaBootstrap, activityTopic, groupId), WatermarkStrategy.noWatermarks(), "activity-source")
                .flatMap((String raw, Collector<MetricDelta> out) -> emitActivityDeltas(raw, out))
                .returns(MetricDelta.class);

        WatermarkStrategy<MetricDelta> watermark = WatermarkStrategy
                .<MetricDelta>forBoundedOutOfOrderness(Duration.ofSeconds(allowedLatenessSeconds))
                .withTimestampAssigner((SerializableTimestampAssigner<MetricDelta>) (event, recordTimestamp) -> event.eventTimeMs);

        DataStream<AggregatedMetric> aggregated = txDeltas
                .union(activityDeltas)
                .assignTimestampsAndWatermarks(watermark)
                .keyBy(v -> v.metricName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(allowedLatenessSeconds))
                .aggregate(new SumMetricAggregate(), new WindowedMetricProjector());

        aggregated.addSink(
                JdbcSink.sink(
                        """
                        INSERT INTO aggregated_metrics (metric_name, metric_value, window_start, window_end)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (metric_name, window_start, window_end)
                        DO UPDATE SET metric_value = EXCLUDED.metric_value
                        """,
                        (statement, metric) -> {
                            statement.setString(1, metric.metricName);
                            statement.setDouble(2, metric.metricValue);
                            statement.setTimestamp(3, Timestamp.from(Instant.ofEpochMilli(metric.windowStartMs)));
                            statement.setTimestamp(4, Timestamp.from(Instant.ofEpochMilli(metric.windowEndMs)));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(500)
                                .withBatchIntervalMs(1000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(jdbcUrl)
                                .withDriverName("org.postgresql.Driver")
                                .withUsername(jdbcUser)
                                .withPassword(jdbcPassword)
                                .build()
                )
        ).name("postgres-aggregated-metrics-sink");

        env.execute("flink-aggregation-job");
    }

    private static KafkaSource<String> kafkaSource(String bootstrap, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId + "-" + topic.replace(".", "-"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private static void emitTransactionDeltas(String raw, Collector<MetricDelta> out) {
        JsonNode node = parse(raw);
        if (node == null) {
            return;
        }
        long eventTime = parseEventTimeMs(node.path("timestamp").asText(null));
        double amount = node.path("amount").asDouble(0.0d);
        out.collect(new MetricDelta("transaction_count", 1.0d, eventTime));
        out.collect(new MetricDelta("total_transaction_amount", amount, eventTime));
    }

    private static void emitActivityDeltas(String raw, Collector<MetricDelta> out) {
        JsonNode node = parse(raw);
        if (node == null) {
            return;
        }
        long eventTime = parseEventTimeMs(node.path("timestamp").asText(null));
        String action = node.path("action").asText(node.path("activity").asText("UNKNOWN"));
        String normalizedAction = action.replaceAll("[^A-Za-z0-9_]", "_").toUpperCase(Locale.ROOT);
        out.collect(new MetricDelta("activity_count", 1.0d, eventTime));
        out.collect(new MetricDelta("activity_count_" + normalizedAction, 1.0d, eventTime));
    }

    private static JsonNode parse(String raw) {
        try {
            return MAPPER.readTree(raw);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static long parseEventTimeMs(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            return Instant.now().toEpochMilli();
        }
        try {
            return Instant.parse(timestamp).toEpochMilli();
        } catch (Exception ignored) {
            return Instant.now().toEpochMilli();
        }
    }

    public static class MetricDelta {
        public String metricName;
        public double delta;
        public long eventTimeMs;

        public MetricDelta() {
        }

        public MetricDelta(String metricName, double delta, long eventTimeMs) {
            this.metricName = metricName;
            this.delta = delta;
            this.eventTimeMs = eventTimeMs;
        }
    }

    public static class AggregatedMetric {
        public String metricName;
        public double metricValue;
        public long windowStartMs;
        public long windowEndMs;

        public AggregatedMetric() {
        }

        public AggregatedMetric(String metricName, double metricValue, long windowStartMs, long windowEndMs) {
            this.metricName = metricName;
            this.metricValue = metricValue;
            this.windowStartMs = windowStartMs;
            this.windowEndMs = windowEndMs;
        }
    }

    public static class SumMetricAggregate implements AggregateFunction<MetricDelta, Double, Double> {
        @Override
        public Double createAccumulator() {
            return 0.0d;
        }

        @Override
        public Double add(MetricDelta value, Double accumulator) {
            return accumulator + value.delta;
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    public static class WindowedMetricProjector extends ProcessWindowFunction<Double, AggregatedMetric, String, TimeWindow> {
        @Override
        public void process(String metricName, Context context, Iterable<Double> elements, Collector<AggregatedMetric> out) {
            Double value = elements.iterator().next();
            out.collect(
                    new AggregatedMetric(
                            metricName,
                            value,
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }
}
