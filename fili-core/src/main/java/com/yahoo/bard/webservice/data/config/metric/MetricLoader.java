// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import org.slf4j.LoggerFactory;

/**
 * Defines the core interactions for loading metrics into a metric dictionary.
 */
public interface MetricLoader {

    /**
     * Load metrics and populate the metric dictionary.
     *
     * @param metricDictionary  The dictionary that will be loaded with metrics
     *
     * @deprecated in favor of loadMetricDictionary(MetricDictionary, DimensionDictionary)
     */
    @Deprecated
    default void loadMetricDictionary(MetricDictionary metricDictionary) {
        String message = "The method loadMetricDictionary(MetricDictionary, DimensionDictionary) should be used " +
                "instead";
        LoggerFactory.getLogger(MetricLoader.class).warn(message);
    }

    /**
     * Load metrics and populate the metric dictionary with dimension dictionary for dimension dependent metrics.
     *
     * @param metricDictionary  The dictionary that will be loaded with metrics
     * @param dimensionDictionary  The dimension dictionary containing loaded dimensions
     */
    default void loadMetricDictionary(MetricDictionary metricDictionary, DimensionDictionary dimensionDictionary) {
        loadMetricDictionary(metricDictionary);
    }
}
