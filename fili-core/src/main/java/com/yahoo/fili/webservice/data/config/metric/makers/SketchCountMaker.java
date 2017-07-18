// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.config.metric.makers;

import com.yahoo.fili.webservice.data.metric.MetricDictionary;
import com.yahoo.fili.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.fili.webservice.data.metric.mappers.SketchRoundUpMapper;
import com.yahoo.fili.webservice.druid.model.aggregation.SketchCountAggregation;

/**
 * Metric maker to wrap sketch metrics into sketch count aggregations.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchMaker
 */

@Deprecated
public class SketchCountMaker extends RawAggregationMetricMaker {

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param sketchSize  The size beyond which the sketch constructed by this maker should perform approximations.
     */
    public SketchCountMaker(MetricDictionary metrics, int sketchSize) {
        super(metrics, ((name, fieldName) -> new SketchCountAggregation(name, fieldName, sketchSize)));
    }

    @Override
    public ResultSetMapper getResultSetMapper(String metricName) {
        return new SketchRoundUpMapper(metricName);
    }
}
