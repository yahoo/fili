// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.util.List;

/**
 * A config-driven MetricLoader.
 */
public class ConfiguredMetricLoader implements MetricLoader {

    private final MetricDictionary localMetricDictionary;
    private final MakerBuilder makerBuilder;
    private final DimensionDictionary dimensionDictionary;
    private final List<MetricConfiguration> metrics;

    /**
     * Construct a new MetricLoader with the given configuration.
     *
     * @param localMetricDictionary  the localMetricDictionary metric dictionary
     * @param metrics  the configured base metrics
     * @param makerBuilder  the metric maker builder
     * @param dimensionDictionary  the dimension dictionary
     */
    public ConfiguredMetricLoader(
            MetricDictionary localMetricDictionary,
            List<MetricConfiguration> metrics,
            MakerBuilder makerBuilder,
            DimensionDictionary dimensionDictionary
    ) {
        this.localMetricDictionary = localMetricDictionary;
        this.dimensionDictionary = dimensionDictionary;
        this.makerBuilder = makerBuilder;
        this.metrics = metrics;
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary) {
        this.localMetricDictionary.clearLocal();

        metrics.stream()
                .map(metric -> metric.build(localMetricDictionary, makerBuilder, dimensionDictionary))
                .forEach(localMetricDictionary::add);

        this.localMetricDictionary.forEach((k, v) -> metricDictionary.add(v));
    }
}
