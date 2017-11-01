// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.config.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.mappers.RowNumMapper;

import java.util.List;

/**
 * Metric maker to provide row nums in result set processing.
 */
public class RowNumMaker extends MetricMaker {

    private static final RowNumMapper ROW_NUM_MAPPER = new RowNumMapper();
    private static final int DEPENDENT_METRICS_REQUIRED = 0;

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     */
    public RowNumMaker(MetricDictionary metrics) {
        super(metrics);
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
        return new LogicalMetric(
                null,
                ROW_NUM_MAPPER,
                metricName,
                "Generator for Row Numbers"
        );
    }

    @Override
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        return new LogicalMetric(
                null,
                ROW_NUM_MAPPER,
                logicalMetricInfo.getName(),
                "Generator for Row Numbers"
        );
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
