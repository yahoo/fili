// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.config.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Metric maker to create a constant value in the post aggregations.
 */
public class ConstantMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 1;
    protected static final Logger LOG = LoggerFactory.getLogger(ConstantMaker.class);

    /**
     * Constructor.
     *
     * @param metricDictionary  Dictionary from which to look up dependent metrics
     */
    public ConstantMaker(MetricDictionary metricDictionary) {
        super(metricDictionary);
    }

    @Override
    public LogicalMetric make(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        // Check that we have the right number of metrics
        assertRequiredDependentMetricCount(logicalMetricInfo.getName(), dependentMetrics);

        // Actually build the metric.
        return makeInner(logicalMetricInfo, dependentMetrics);
    }

    @Override
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        String metricName = logicalMetricInfo.getName();
        try {
            Set<PostAggregation> postAggregations = Collections.singleton(new ConstantPostAggregation(
                    metricName,
                    new Double(dependentMetrics.get(0))
            ));

            return new LogicalMetric(
                    new TemplateDruidQuery(Collections.emptySet(), postAggregations),
                    NO_OP_MAPPER,
                    logicalMetricInfo
            );
        } catch (NumberFormatException nfe) {
            String message = String.format(
                    "%s value '%s' does not parse to a number",
                    metricName,
                    dependentMetrics.get(0)
            );
            LOG.error(message);
            throw new IllegalArgumentException(message, nfe);
        }
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
