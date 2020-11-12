// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric;


import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_BOOLEAN_METRIC;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_HEIGHT;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_JSON_NODE_METRIC;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_NULL_METRIC;
import static com.yahoo.bard.webservice.data.config.names.TestApiMetricName.A_STRING_METRIC;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSetSchema;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.druid.model.aggregation.LongMinAggregation;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Used by tests. Provides a wrapper around a static method for getting stubbed LogicalMetrics of complex metrics,
 * and simple Mappers that illustrate how one might leverage the non-numeric metrics.
 */
public class NonNumericMetrics {

    public static final String UNEXPECTED_MISSING_COLUMN = "Unexpected missing column ";

    /**
     * Returns a list of LogicalMetrics whose names are those intended to represent non-numeric metrics.
     *
     * @return A list of LogicalMetrics whose names are those intended to represent non-numeric metrics.
     */
    public static List<LogicalMetric> getLogicalMetrics() {
        return Arrays.asList(
                new LogicalMetricImpl(
                    new TemplateDruidQuery(
                            /*
                             * Placeholder so that missing intervals behaves nicely. Also, giving the aggregation
                             * the same name as the complex metric so that the correct metric is extracted from the
                             * injected query.
                             */
                            Collections.singleton(new LongMinAggregation(
                                    A_STRING_METRIC.asName(),
                                    A_HEIGHT.getApiName()
                            )),
                            Collections.emptySet()
                    ),
                    new StringMetricMapper(),
                    new LogicalMetricInfo(
                            A_STRING_METRIC.getApiName(),
                            A_STRING_METRIC.getApiName(),
                            "ImAStringISwear"
                    )
            ),
            new LogicalMetricImpl(
                    new TemplateDruidQuery(
                            Collections.singleton(new LongMinAggregation(
                                    A_BOOLEAN_METRIC.asName(),
                                    A_HEIGHT.getApiName()
                            )),
                            Collections.emptySet()
                    ),
                    new BooleanMetricMapper(),
                    new LogicalMetricInfo(
                            A_BOOLEAN_METRIC.getApiName(),
                            A_BOOLEAN_METRIC.getApiName(),
                            "ImBooleanISwear")
            ),
            new LogicalMetricImpl(
                    new TemplateDruidQuery(
                            Collections.singleton(
                                    new LongMinAggregation(A_JSON_NODE_METRIC.asName(), A_HEIGHT.getApiName())
                            ),
                            Collections.emptySet()
                    ),
                    new JsonNodeMetricMapper(),
                    new LogicalMetricInfo(
                            A_JSON_NODE_METRIC.getApiName(),
                            A_JSON_NODE_METRIC.getApiName(),
                            "ImAJsonNodeISwear"
                    )
            ),
            new LogicalMetricImpl(
                    new TemplateDruidQuery(
                            Collections.singleton(new LongMinAggregation(
                                    A_NULL_METRIC.asName(),
                                    A_HEIGHT.getApiName()
                            )),
                            Collections.emptySet()
                    ),
                    new NullMetricMapper(),
                    new LogicalMetricInfo(
                            A_NULL_METRIC.getApiName(),
                            A_NULL_METRIC.getApiName(),
                            "ImNullISwear"
                    )
            )
        );
    }

     /**
     * Sample mapper for handling String metrics. Just duplicates the String.
     */
    private static class StringMetricMapper extends ResultSetMapper {

        /**
         * Constructor.
         */
        StringMetricMapper() {
            super();
        }

        @Override
        protected Result map(Result result, ResultSetSchema schema) {
            MetricColumn stringColumn = schema.getColumn(A_STRING_METRIC.asName(), MetricColumn.class)
                    .orElseThrow(
                            () -> new IllegalStateException(UNEXPECTED_MISSING_COLUMN + A_STRING_METRIC.asName())
            );
            String stringValue = result.getMetricValueAsString(stringColumn);
            return result.withMetricValue(stringColumn, stringValue + stringValue);
        }

        @Override
        protected ResultSetSchema map(ResultSetSchema schema) {
            return schema;
        }
    }

    /**
     * Sample mapper for handling boolean metrics. Just filters out any results whose boolean field is false.
     */
    private static class BooleanMetricMapper extends ResultSetMapper {

        /**
         * Constructor.
         */
        BooleanMetricMapper() {
            super();
        }

        @Override
        protected Result map(Result result, ResultSetSchema schema) {
            MetricColumn column = schema.getColumn(A_BOOLEAN_METRIC.asName(), MetricColumn.class).orElseThrow(
                    () -> new IllegalStateException(UNEXPECTED_MISSING_COLUMN + A_BOOLEAN_METRIC.asName())
            );
            return result.getMetricValueAsBoolean(column) ? result : null;
        }

        @Override
        protected ResultSetSchema map(ResultSetSchema schema) {
            return schema;
        }
    }

    /**
     * Sample mapper for handling JsonNode metrics. Adds a new field to the node whose value is the length of the
     * 'clarification' field.
     */
    private static class JsonNodeMetricMapper extends ResultSetMapper {

        /**
         * Constructor.
         */
        JsonNodeMetricMapper() {
            super();
        }

        @Override
        protected Result map(Result result, ResultSetSchema schema) {
            MetricColumn column = schema.getColumn(A_JSON_NODE_METRIC.asName(), MetricColumn.class).orElseThrow(
                    () -> new IllegalStateException(UNEXPECTED_MISSING_COLUMN + A_JSON_NODE_METRIC.asName())
            );
            ObjectNode node = (ObjectNode) result.getMetricValueAsJsonNode(column);
            node.put("length", node.get("clarification").textValue().length());
            return result;
        }

        @Override
        protected ResultSetSchema map(ResultSetSchema schema) {
            return schema;
        }
    }

    /**
     * Sample mapper for handling null values. Throws an IllegalStateException if the metric 'nullMetric' is not null.
     */
    private static class NullMetricMapper extends ResultSetMapper {

        /**
         * Constructor.
         */
        NullMetricMapper() {
            super();
        }

        @Override
        protected Result map(Result result, ResultSetSchema schema) {
            MetricColumn column = schema.getColumn(A_NULL_METRIC.asName(), MetricColumn.class).orElseThrow(
                    () -> new IllegalStateException(UNEXPECTED_MISSING_COLUMN + A_NULL_METRIC.asName())
            );

            Object nullMetric = result.getMetricValue(column);
            if (nullMetric != null) {
                throw new IllegalStateException(
                        String.format("Metric 'nullMetric' should be null but is: %s", nullMetric)
                );
            }
            return result;
        }

        @Override
        protected ResultSetSchema map(ResultSetSchema schema) {
            return schema;
        }
    }
}
