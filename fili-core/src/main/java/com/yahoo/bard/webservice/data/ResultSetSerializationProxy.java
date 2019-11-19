// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.Schema;
import com.yahoo.bard.webservice.util.DateTimeUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Simplified version of ResultSet class for json format serialization.
 */
public class ResultSetSerializationProxy {

    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String RESULTS_KEY = "results";
    public static final String SCHEMA_KEY = "schema";

    public static final String SCHEMA_TIMEZONE = "timeZone";
    public static final String SCHEMA_GRANULARITY = "granularity";
    public static final String SCHEMA_DIM_COLUMNS = "dimensionColumns";
    public static final String SCHEMA_METRIC_COLUMNS = "metricColumns";
    public static final String SCHEMA_METRIC_COLUMNS_TYPE = "metricColumnsType";

    public static final String DEFAULT_CLASS_TYPE = "java.lang.String";

    private final Map<String, Object> serializedSchema;
    private final List<ResultSerializationProxy> resultSerializationProxies;

    /**
     * Constructor.
     *
     * @param resultSet  The ResultSet to prepare for serialization
     */
    public ResultSetSerializationProxy(ResultSet resultSet) {

        resultSerializationProxies = new ArrayList<>();
        Map<String, String> metricValuesClassType = new HashMap<>();
        Set<String> metricNames = getMetricColumnNames(resultSet.getSchema());

        for (Result result : resultSet) {
            //Custom serialization for each result
            ResultSerializationProxy resultProxy = new ResultSerializationProxy(result);
            resultSerializationProxies.add(resultProxy);

            //Copying all the metric names to new set to avoid concurrent ConcurrentModification exception
            Set<String> unknownMetricTypes = new HashSet<>(metricNames);

            //For each metric name find its value type from result row
            if (!unknownMetricTypes.isEmpty()) {
                for (String metricName : unknownMetricTypes) {
                    String classTypeName = resultProxy.getMetricValuesType().get(metricName);
                    if (classTypeName != null) {
                        metricValuesClassType.put(metricName, classTypeName);
                        metricNames.remove(metricName);
                    }
                }
            }
        }

        //Consider default type for the metric name when their respective types are not available from the results
        metricNames.stream().forEach(metricName -> metricValuesClassType.put(metricName, DEFAULT_CLASS_TYPE));

        this.serializedSchema = getSchemaComponents(resultSet.getSchema());
        this.serializedSchema.put(SCHEMA_METRIC_COLUMNS_TYPE, metricValuesClassType);
    }

    @JsonProperty(RESULTS_KEY)
    public List<ResultSerializationProxy> getResultSerializationProxies() {
        return resultSerializationProxies;
    }

    @JsonProperty(SCHEMA_KEY)
    public Map<String, Object> getSerializedSchema() {
        return serializedSchema;
    }

    /**
     * Extract schema components from ResultSet schema.
     *
     * @param schema  Schema object from the ResultSet
     *
     * @return Schema components.
     */
    private Map<String, Object> getSchemaComponents(ResultSetSchema schema) {
        Map<String, Object> schemaComponents = new HashMap<>();

        schemaComponents.put(SCHEMA_TIMEZONE, DateTimeUtils.getTimeZone(schema.getGranularity()).getID());
        schemaComponents.put(SCHEMA_GRANULARITY, schema.getGranularity().getName());
        schemaComponents.put(
                SCHEMA_DIM_COLUMNS,
                schema.getColumns(DimensionColumn.class)
                        .stream()
                        .map(Column::getName)
                        .collect(Collectors.toCollection(LinkedHashSet::new))
        );
        schemaComponents.put(
                SCHEMA_METRIC_COLUMNS,
                getMetricColumnNames(schema)
        );

        return schemaComponents;
    }

    /**
     * Get the names of the metric columns from the Schema.
     *
     * @param schema  Schema to extract the names from
     *
     * @return the names of the metric columns
     */
    private Set<String> getMetricColumnNames(Schema schema) {
        return schema.getColumns(MetricColumn.class).stream().map(Column::getName).collect(Collectors.toSet());
    }
}
