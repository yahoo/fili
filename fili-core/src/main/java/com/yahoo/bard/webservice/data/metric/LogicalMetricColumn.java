// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import com.yahoo.bard.webservice.table.Schema;

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
     */
    public LogicalMetricColumn(String name, LogicalMetric metric) {
        super(name);
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

    /**
     * Method to create a LogicalMetricColumn tied to a schema.
     *
     * @param schema  The associated schema
     * @param name  The metric name
     * @param metric  The logical metric
     *
     * @return DimensionColumn created
     */
    public static LogicalMetricColumn addNewLogicalMetricColumn(Schema schema, String name, LogicalMetric metric) {
        LogicalMetricColumn col = new LogicalMetricColumn(name, metric);
        schema.addColumn(col);
        return col;
    }

    @Override
    public String toString() {
        return "{logicalMetric:'" + getName() + "'}";
    }
}
