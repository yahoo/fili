// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.util.List;

/**
 * Metric maker which only creates aggregations.  They do not depend on other metrics and cannot be
 * validated against the metric dictionary
 */
public abstract class RawAggregationMetricMaker extends MetricMaker {

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     */
    public RawAggregationMetricMaker(MetricDictionary metrics) {
        super(metrics);
    }

    /**
     * Aggregation columns are not expected to be in the metric Dictionary.
     *
     * @param dependentMetrics  List of dependent metrics needed to check
     */
    @Override
    protected void assertDependentMetricsExist(List<String> dependentMetrics) {
    }
}
