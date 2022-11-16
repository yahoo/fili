// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
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
    // TODO Determine if this is useful.  Object equality on LogicalMetric
    // seems like it might make it useless
    public List<LogicalTable> findByLogicalMetric(LogicalMetric logicalMetric) {
        return values()
                .stream()
                .filter(it -> it.getLogicalMetrics().contains(logicalMetric))
                .collect(Collectors.toList());
    }

    /**
     * Get the logical tables for which the given logical metric name is valid.
     *
     * @param logicalMetricName  Logical Metric Name to look up Logical Tables by
     *
     * @return The list of logical tables that have the logical metric name
     */
    public Set<LogicalTable> findByLogicalMetricName(String logicalMetricName) {
        Set<LogicalTable> logicalTableSet = new HashSet<>();
        for (LogicalTable table : values()) {
            Set<LogicalMetric> logicalMetricSet = table.getLogicalMetrics();
            for (LogicalMetric lm : logicalMetricSet) {
                if (lm.getName().equals(logicalMetricName)) {
                    logicalTableSet.add(table);
                }
            }
        }
        return logicalTableSet;
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
