// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.config.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation;
import com.yahoo.bard.webservice.druid.model.filter.Filter;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Build a Filtered Aggregation logical metric.
 * This metric maker takes a metric whose field is an aggregator and builds a metric with the same dependencies except
 * without the aggregation and with a filtered aggregation instead using a filter fixed at construction.
 */
public class FilteredAggregationMaker extends MetricMaker {

    protected static final Logger LOG = LoggerFactory.getLogger(FilteredAggregationMaker.class);

    /**
     * The filter being applied to the metrics created.
     */
    private final Filter filter;

    /**
     * The metric dictionary used to resolve dependent metrics.
     */
    private final MetricDictionary metricDictionary;

    /**
     * Construct a metric maker for Filtered Aggregations.
     *
     * @param metricDictionary the metric dictionary
     * @param filter the filter to filter the aggregation by
     */
    public FilteredAggregationMaker(MetricDictionary metricDictionary, Filter filter) {
        super(metricDictionary);
        this.filter = filter;
        this.metricDictionary = metricDictionary;
    }

    @Override
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        LogicalMetric sourceMetric = metricDictionary.get(dependentMetrics.get(0));

        Aggregation sourceAggregation = assertDependentIsAggregationMetric(sourceMetric);

        FilteredAggregation filteredAggregation = new FilteredAggregation(
                logicalMetricInfo.getName(),
                sourceAggregation,
                filter
        );

        return new LogicalMetric(
                new TemplateDruidQuery(
                        ImmutableSet.of(filteredAggregation),
                        Collections.emptySet(),
                        sourceMetric.getTemplateDruidQuery().getInnerQuery().orElse(null)
                ),
                sourceMetric.getCalculation(),
                logicalMetricInfo
        );
    }

    /**
     * Test that the metric is associated with an Aggregation Metric and return that Aggregation.
     *
     * @param sourceMetric  The source metric
     *
     * @return The aggregation used by this metric
     */
    private Aggregation assertDependentIsAggregationMetric(LogicalMetric sourceMetric) {
        MetricField metricField = sourceMetric.getMetricField();
        if (!(metricField instanceof Aggregation)) {
            String message = String.format(
                    "FilteredAggregationMaker requires an aggregation metric, but found: %s.",
                    sourceMetric.getName()
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
        return (Aggregation) metricField;
    }

    @Override
    protected int getDependentMetricsRequired() {
        return 1;
    }
}
