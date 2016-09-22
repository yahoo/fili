// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Metric maker to sum over numeric metrics.
 */
public class DoubleSumMaker extends RawAggregationMetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 1;

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     */
    public DoubleSumMaker(MetricDictionary metrics) {
        super(metrics);
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {

        String dependentMetric = dependentMetrics.get(0);

        DoubleSumAggregation agg = new DoubleSumAggregation(metricName, dependentMetric);

        Set<Aggregation> aggs = Collections.singleton(agg);
        Set<PostAggregation> postAggs = Collections.emptySet();

        TemplateDruidQuery query = new TemplateDruidQuery(aggs, postAggs);

        return new LogicalMetric(query, new NoOpResultSetMapper(), metricName);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
