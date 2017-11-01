// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.config.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.aggregation.CountAggregation;

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
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        TemplateDruidQuery query = new TemplateDruidQuery(
                Collections.singleton(new CountAggregation(logicalMetricInfo.getName())),
                Collections.emptySet()
        );

        return new LogicalMetric(query, NO_OP_MAPPER, logicalMetricInfo);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
