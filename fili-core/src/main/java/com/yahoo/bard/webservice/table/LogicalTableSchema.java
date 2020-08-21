// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The schema for a logical table.
 */
public class LogicalTableSchema extends BaseSchema {

    /**
     * Constructor.
     *
     * @param tableGroup  The table group used to initial this logical table
     * @param granularity  The granularity for this schema
     * @param metricDictionary  The dictionary to resolve metric names from the table group against
     */
    public LogicalTableSchema(TableGroup tableGroup, Granularity granularity, MetricDictionary metricDictionary) {
        super(granularity, buildLogicalColumns(tableGroup, granularity, metricDictionary));
    }

    /**
     * Copy Constructor.
     *
     * @param granularity  The granularity for this schema.
     * @param columns  The columns for this schema.
     */
    protected LogicalTableSchema(Granularity granularity, Iterable<Column> columns) {
        super(granularity, columns);
    }

    /**
     * Convert the tables in the table group to a set of dimension and metric columns.
     *
     * @param tableGroup  The collection of table group physical tables.
     * @param granularity  The granularity for this schema
     * @param metricDictionary  The dictionary to build logical metrics from names.
     *
     * @return The union of all columns from the table group
     */
    private static LinkedHashSet<Column> buildLogicalColumns(
            TableGroup tableGroup,
            Granularity granularity,
            MetricDictionary metricDictionary
    ) {
        return Stream.concat(
                tableGroup.getDimensions().stream()
                        .map(DimensionColumn::new),
                buildMetricColumns(tableGroup.getApiMetricNames(), granularity, metricDictionary)
        ).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Buid a stream of metric columns by filtering the table group's collection of ApiMetricNames.
     * Metric names are resolved and then filtered considering the table granularity and the metric referenced.
     *
     * @param apiMetricNames  The names of the apiMetrics being bound and filtered
     * @param granularity  The grain used to filter those metric names
     * @param metricDictionary  The dictionary to resolve the logical metric instances from
     *
     * @return A stream of metric columns, filtered for compatibility with the grain.
     */
    private static Stream<LogicalMetricColumn> buildMetricColumns(
            Collection<ApiMetricName> apiMetricNames,
            Granularity granularity,
            MetricDictionary metricDictionary
    ) {
        return apiMetricNames.stream()
                .filter(name -> name.isValidFor(granularity, metricDictionary.get(name.asName())))
                .map(ApiMetricName::asName)
                .map(metricDictionary::get)
                .map(LogicalMetricColumn::new);
    }
}
