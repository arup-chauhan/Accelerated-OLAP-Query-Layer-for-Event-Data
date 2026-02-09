package com.engine.query.service;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

@Component
@ConditionalOnProperty(
        name = "engine.query.validate-aggregate-schema",
        havingValue = "true",
        matchIfMissing = true
)
public class AggregatedSchemaValidator {

    private static final Logger log = LoggerFactory.getLogger(AggregatedSchemaValidator.class);

    private final DataSource dataSource;

    public AggregatedSchemaValidator(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void validate() {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            validateTable(metaData, "aggregated_metrics");
            validateColumns(metaData, "aggregated_metrics", Set.of(
                    "metric_name",
                    "metric_value",
                    "window_start",
                    "window_end"
            ));
            log.info("Validated query serving schema for table aggregated_metrics");
        } catch (Exception e) {
            throw new IllegalStateException("Aggregate schema validation failed for aggregated_metrics", e);
        }
    }

    private void validateTable(DatabaseMetaData metaData, String tableName) throws Exception {
        try (ResultSet rs = metaData.getTables(null, null, tableName, new String[]{"TABLE"})) {
            if (!rs.next()) {
                throw new IllegalStateException("Missing required table: " + tableName);
            }
        }
    }

    private void validateColumns(DatabaseMetaData metaData, String tableName, Set<String> requiredColumns) throws Exception {
        Set<String> foundColumns = new HashSet<>();
        try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                String column = rs.getString("COLUMN_NAME");
                if (column != null) {
                    foundColumns.add(column.toLowerCase(Locale.ROOT));
                }
            }
        }

        Set<String> missing = new HashSet<>();
        for (String required : requiredColumns) {
            if (!foundColumns.contains(required.toLowerCase(Locale.ROOT))) {
                missing.add(required);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("Missing required columns in " + tableName + ": " + missing);
        }
    }
}
