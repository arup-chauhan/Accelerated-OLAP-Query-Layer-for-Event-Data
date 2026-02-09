package com.engine.query.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AggregatedSchemaValidatorTest {

    @Mock
    private DataSource dataSource;
    @Mock
    private Connection connection;
    @Mock
    private DatabaseMetaData metaData;
    @Mock
    private ResultSet tableRs;
    @Mock
    private ResultSet columnRs;

    private AggregatedSchemaValidator validator;

    @BeforeEach
    void setUp() throws Exception {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(null, null, "aggregated_metrics", new String[]{"TABLE"})).thenReturn(tableRs);
        when(metaData.getColumns(null, null, "aggregated_metrics", null)).thenReturn(columnRs);
        validator = new AggregatedSchemaValidator(dataSource);
    }

    @Test
    void validatePassesWhenTableAndColumnsExist() throws Exception {
        when(tableRs.next()).thenReturn(true);
        when(columnRs.next()).thenReturn(true, true, true, true, false);
        when(columnRs.getString("COLUMN_NAME")).thenReturn(
                "metric_name", "metric_value", "window_start", "window_end"
        );

        validator.validate();
    }

    @Test
    void validateFailsWhenRequiredColumnMissing() throws Exception {
        when(tableRs.next()).thenReturn(true);
        when(columnRs.next()).thenReturn(true, true, true, false);
        when(columnRs.getString("COLUMN_NAME")).thenReturn(
                "metric_name", "metric_value", "window_start"
        );

        assertThatThrownBy(() -> validator.validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Aggregate schema validation failed");
    }
}
