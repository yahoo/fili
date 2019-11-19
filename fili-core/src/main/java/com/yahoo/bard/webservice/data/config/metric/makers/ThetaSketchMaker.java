// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.data.metric.mappers.SketchRoundUpMapper;
import com.yahoo.bard.webservice.druid.model.aggregation.ThetaSketchAggregation;

/**
 * Metric maker to wrap sketch metric aggregations.
 */
public class ThetaSketchMaker extends RawAggregationMetricMaker {

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param sketchSize  The size beyond which the sketch constructed by this maker should perform approximations.
     */
    public ThetaSketchMaker(MetricDictionary metrics, int sketchSize) {
        super(metrics, (name, fieldName) -> new ThetaSketchAggregation(name, fieldName, sketchSize));
    }

    @Override
    public ResultSetMapper getResultSetMapper(String metricName) {
        return new SketchRoundUpMapper(metricName);
    }
}
