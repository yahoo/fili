// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.aggregation.MaxAggregation;

/**
 * Metric maker to get the max of numeric metrics
 * <p>
 * Druid now supports separate maximum aggregations for Long and Double instead of a single maximum for both. Therefore,
 * {@link LongMaxMaker} and {@link DoubleMaxMaker} should be used instead.
 */
@Deprecated
public class MaxMaker extends RawAggregationMetricMaker {
    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics
     */
    public MaxMaker(MetricDictionary metrics) {
        super(metrics, MaxAggregation::new);
    }
}
