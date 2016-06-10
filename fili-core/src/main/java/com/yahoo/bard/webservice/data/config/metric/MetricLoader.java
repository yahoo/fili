// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.metric.MetricDictionary;

/**
 * Defines the core interactions for loading metrics into a metric dictionary.
 */
public interface MetricLoader {

    /**
     * Load metrics and populate the metric dictionary
     *
     * @param metricDictionary  The dictionary that will be loaded with metrics
     */
    void loadMetricDictionary(MetricDictionary metricDictionary);

}
