// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The schema for a logical table.
 */
public class LogicalTableSchema extends BaseSchema  {

    /**
     * Constructor.
     *
     * @param tableGroup  The table group used to initial this logical table
     * @param metricDictionary  The dictionary to resolve metric names from the table group against
     */
    public LogicalTableSchema(TableGroup tableGroup, MetricDictionary metricDictionary) {
        super(toColumns(tableGroup, metricDictionary));
    }

    /**
     * Convert the tables in the table group to a set of dimension and metric columns.
     *
     * @param tableGroup  The collection of table group physical tables.
     * @param metricDictionary  The dictionary to build logical metrics from names.
     *
     * @return  The union of all columns from the table group
     */
    private static Set<Column> toColumns(TableGroup tableGroup, MetricDictionary metricDictionary) {
        return Stream.concat(
                tableGroup.getDimensions().stream()
                .map(DimensionColumn::new),
                tableGroup.getApiMetricNames().stream()
                        .map(ApiMetricName::getApiName)
                .map(name -> new LogicalMetricColumn(name, metricDictionary.get(name)))
        ).collect(Collectors.toSet());
    }
}
