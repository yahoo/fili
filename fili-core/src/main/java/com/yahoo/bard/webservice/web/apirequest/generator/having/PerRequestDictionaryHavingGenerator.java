// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.having;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.web.ApiHaving;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A having generator decorator that uses the metrics from the query as the dictionary of metrics for building havings.
 */
public class PerRequestDictionaryHavingGenerator implements HavingGenerator {

    private final HavingGenerator havingGenerator;

    /**
     * Constructor.
     *
     * @param havingGenerator  A default having generator to decorate.
     */
    public PerRequestDictionaryHavingGenerator(HavingGenerator havingGenerator) {
        this.havingGenerator = havingGenerator;
    }

    /**
     * Wrap the enclosed having generator in a query scoped metric dictionary.
     *
     * @param havingString  The having clause from the URI
     * @param logicalMetrics The set of metrics provided
     *
     * @return  A collection of ApiHaving objects grouped by Logical Metric
     */
    @Override
    public Map<LogicalMetric, Set<ApiHaving>> apply(String havingString, Set<LogicalMetric> logicalMetrics) {
        Map<String, LogicalMetric> metricMap = logicalMetrics.stream()
                .collect(Collectors.toMap(LogicalMetric::getName, Function.identity()));
        MetricDictionary metricDictionary = new MetricDictionary();
        metricDictionary.putAll(metricMap);
        return havingGenerator
                .withMetricDictionary(metricDictionary)
                .apply(havingString, logicalMetrics);
    }

    @Override
    public HavingGenerator withMetricDictionary(MetricDictionary metricDictionary) {
        return new PerRequestDictionaryHavingGenerator(havingGenerator.withMetricDictionary(metricDictionary));
    }
}
