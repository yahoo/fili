// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.config.metric.makers;

import com.yahoo.fili.webservice.data.metric.LogicalMetric;
import com.yahoo.fili.webservice.data.metric.MetricDictionary;
import com.yahoo.fili.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.fili.webservice.druid.model.aggregation.CountAggregation;

import java.util.Collections;
import java.util.List;

/**
 * Metric maker to count rows that match the filters.
 */
public class CountMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 0;

    /**
     * Constructor.
     *
     * @param metricDictionary  Dictionary from which to look up dependent metrics
     */
    public CountMaker(MetricDictionary metricDictionary) {
        super(metricDictionary);
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
        TemplateDruidQuery query = new TemplateDruidQuery(
                Collections.singleton(new CountAggregation(metricName)),
                Collections.emptySet()
        );

        return new LogicalMetric(query, NO_OP_MAPPER, metricName);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
