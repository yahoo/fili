// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.provider.descriptor.MetricDescriptor;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.util.List;

/**
 * A config-driven MetricLoader.
 */
public class ConfiguredMetricLoader implements MetricLoader {

    private final List<MetricDescriptor> metrics;
    private final LogicalMetricBuilder builder;

    /**
     * Construct a new MetricLoader with the given configuration.
     *
     * @param metrics  The configured base metrics
     * @param metricBuilder  The metric builder
     */
    public ConfiguredMetricLoader(
            List<MetricDescriptor> metrics,
            LogicalMetricBuilder metricBuilder
    ) {
        this.metrics = metrics;
        this.builder = metricBuilder;
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary) {
        metrics.stream()
                .map(builder::buildMetric)
                .forEach(metricDictionary::add);
    }
}
