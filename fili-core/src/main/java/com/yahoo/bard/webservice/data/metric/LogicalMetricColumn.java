// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

/**
 * LogicalMetricColumn.
 */
public class LogicalMetricColumn extends MetricColumn {

    private final LogicalMetric metric;

    /**
     * Constructor.
     *
     * @param name  The column name
     * @param metric  The logical metric
     *
     * @deprecated because LogicalMetricColumn is really only a thing for LogicalTable, so there's no reason for there
     * to be an alias on the LogicalMetric inside the LogicalTableSchema.
     */
    @Deprecated
    public LogicalMetricColumn(String name, LogicalMetric metric) {
        super(name);
        this.metric = metric;
    }

    /**
     * Constructor.
     *
     * @param metric  The logical metric
     */
    public LogicalMetricColumn(LogicalMetric metric) {
        super(metric.getName());
        this.metric = metric;
    }

    /**
     * Getter for a logical metric.
     *
     * @return logical metric
     */
    public LogicalMetric getLogicalMetric() {
        return this.metric;
    }

    @Override
    public String toString() {
        return "{logicalMetric:'" + getName() + "'}";
    }
}
