// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.aggregation.MinAggregation;

/**
 * Metric maker to get the min of numeric metrics.
 * <p>
 * Druid now supports separate minimum aggregations for Long and Double instead of a single minimum. Therefore,
 * {@link LongMinMaker} and {@link DoubleMinMaker} should be used instead.
 */
@Deprecated
public class MinMaker extends RawAggregationMetricMaker {
    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics
     */
    public MinMaker(MetricDictionary metrics) {
        super(metrics, MinAggregation::new);
    }
}
