// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation;

/**
 * Metric maker to sum over numeric metrics.
 */
public class DoubleSumMaker extends RawAggregationMetricMaker {

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics
     */
    public DoubleSumMaker(MetricDictionary metrics) {
        super(metrics, DoubleSumAggregation::new);
    }
}
