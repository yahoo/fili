// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.SketchRoundUpMapper;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.SketchCountAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Metric maker to wrap sketch metrics into sketch count aggregations.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchMaker
 */

@Deprecated
public class SketchCountMaker extends RawAggregationMetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 1;

    final int sketchSize;

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param sketchSize  The size beyond which the sketch constructed by this maker should perform approximations.
     */
    public SketchCountMaker(MetricDictionary metrics, int sketchSize) {
        super(metrics);
        this.sketchSize = sketchSize;
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {

        String dependentMetric = dependentMetrics.get(0);

        SketchCountAggregation agg = new SketchCountAggregation(metricName, dependentMetric, sketchSize);

        Set<Aggregation> aggs = Collections.singleton(agg);
        Set<PostAggregation> postAggs = Collections.emptySet();

        TemplateDruidQuery query = new TemplateDruidQuery(aggs, postAggs);

        return new LogicalMetric(query, new SketchRoundUpMapper(metricName), metricName);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
