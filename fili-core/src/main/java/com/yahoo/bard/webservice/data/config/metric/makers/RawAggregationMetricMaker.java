// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.config.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Metric maker which only creates aggregations.  They do not depend on other metrics and cannot be
 * validated against the metric dictionary
 */
public abstract class RawAggregationMetricMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 1;

    private final BiFunction<String, String, Aggregation> aggregationFactory;

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param aggregationFactory  Produce an aggregation from a name and field name
     */
    public RawAggregationMetricMaker(
            MetricDictionary metrics,
            BiFunction<String, String, Aggregation> aggregationFactory
    ) {
        super(metrics);
        this.aggregationFactory = aggregationFactory;
    }

    /**
     * Aggregation columns are not expected to be in the metric Dictionary.
     *
     * @param dependentMetrics  List of dependent metrics needed to check
     */
    @Override
    protected void assertDependentMetricsExist(List<String> dependentMetrics) {
    }


    @Override
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        String metricName = logicalMetricInfo.getName();
        Aggregation aggregation = aggregationFactory.apply(metricName, dependentMetrics.get(0));
        return new LogicalMetric(
                new TemplateDruidQuery(Collections.singleton(aggregation), Collections.emptySet()),
                getResultSetMapper(metricName),
                logicalMetricInfo
        );
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }

    /**
     * The result set mapper associated with this metric.
     *
     * @param metricName  The metric name modified by this result set mapper (if any)
     *
     * @return The result set mapper bound to a metric being produced by this maker.
     */
    protected ResultSetMapper getResultSetMapper(String metricName) {
        return NO_OP_MAPPER;
    }
}
