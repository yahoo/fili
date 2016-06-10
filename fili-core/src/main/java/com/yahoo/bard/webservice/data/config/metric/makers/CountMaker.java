// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.CountAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Metric maker to count rows that match the filters
 */
public class CountMaker extends RawAggregationMetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 0;

    public CountMaker(MetricDictionary metricName) {
        super(metricName);
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
        CountAggregation agg = new CountAggregation(metricName);

        Set<Aggregation> aggs = new LinkedHashSet<>(Arrays.asList(agg));
        Set<PostAggregation> postAggs = Collections.emptySet();

        TemplateDruidQuery query = new TemplateDruidQuery(aggs, postAggs);

        return new LogicalMetric(query, new NoOpResultSetMapper(), metricName);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
