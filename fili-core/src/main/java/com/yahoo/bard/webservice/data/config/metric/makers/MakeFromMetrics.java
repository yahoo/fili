// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Interface to indicate a metric make supports building from resolved metrics without use of a metric dictionary.
 */
public interface MakeFromMetrics {

    /**
     * Delegated to for actually making the metric after building dependencies.
     *
     * @param logicalMetricInfo  Logical metric info provider
     * @param dependentMetrics  Metrics this metric depends on
     *
     * @return the new logicalMetric
     */
    LogicalMetric makeInnerWithResolvedDependencies(
            LogicalMetricInfo logicalMetricInfo,
            List<LogicalMetric> dependentMetrics
    );

    /**
     * A naive implementation for resolving dependencies.
     *
     * @param metricDictionary  The metric dictionary to resolve logical names against
     * @param logicalNames  The names for metrics to retrieve from the dictionary.
     *
     * @return the resolved metrics.
     */
    default List<LogicalMetric> resolveDependencies(MetricDictionary metricDictionary, List<String> logicalNames) {
        return logicalNames.stream().map(metricDictionary::get).collect(Collectors.toList());
    }
}
