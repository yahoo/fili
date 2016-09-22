// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Metric maker to create a constant value in the post aggregations.
 */
public class ConstantMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 1;

    /**
     * Constructor.
     *
     * @param metricDictionary  Dictionary from which to look up dependent metrics
     */
    public ConstantMaker(MetricDictionary metricDictionary) {
        super(metricDictionary);
    }

    @Override
    public LogicalMetric make(String metricName, List<String> dependentMetrics) {
        // Check that we have the right number of metrics
        assertRequiredDependentMetricCount(metricName, dependentMetrics);

        // Actually build the metric.
        return makeInner(metricName, dependentMetrics);
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {

        Double number = new Double(dependentMetrics.get(0));

        ConstantPostAggregation postAgg = new ConstantPostAggregation(metricName, number);

        Set<Aggregation> aggs = Collections.emptySet();
        Set<PostAggregation> postAggs = Collections.singleton(postAgg);

        TemplateDruidQuery query = new TemplateDruidQuery(aggs, postAggs);

        return new LogicalMetric(query, new NoOpResultSetMapper(), metricName);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
