// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Singleton;

/**
 * Logical Table dictionary facilitates the mapping of the logical table Name and time grain pair to the logical table
 * object.
 */
@Singleton
public class LogicalTableDictionary extends LinkedHashMap<TableIdentifier, LogicalTable> {

    /**
     * Get the logical tables for which the given logical metric is valid.
     *
     * @param logicalMetric  Logical Metric to look up Logical Tables by
     *
     * @return The list of logical tables that have the logical metric
     */
    public List<LogicalTable> findByLogicalMetric(LogicalMetric logicalMetric) {
        return values()
                .stream()
                .filter(it -> it.getLogicalMetrics().contains(logicalMetric))
                .collect(Collectors.toList());
    }

    /**
     * Get the logical tables for which the given logical dimension is valid.
     *
     * @param dimension  Dimension to look up Logical Tables by
     *
     * @return The list of logical tables that have the dimension
     */
    public List<LogicalTable> findByDimension(Dimension dimension) {
        return values()
                .stream()
                .filter(it -> it.getDimensions().contains(dimension))
                .collect(Collectors.toList());
    }
}
