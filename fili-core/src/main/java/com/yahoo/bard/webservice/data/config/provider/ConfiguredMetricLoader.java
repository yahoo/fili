// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.util.Map;

/**
 * A config-driven MetricLoader.
 */
public class ConfiguredMetricLoader implements MetricLoader {

    private final MetricDictionary localDictionary;
    private final MakerBuilder makerBuilder;
    private final DimensionDictionary dimensionDictionary;
    private final ConfigurationDictionary<MetricConfiguration> baseMetrics;
    private final ConfigurationDictionary<MetricConfiguration> derivedMetrics;

    /**
     * Construct a new MetricLoader with the given configuration.
     *
     * @param localDictionary the local metric dictionary
     * @param baseMetrics the configured base metrics
     * @param derivedMetrics the configured derived metrics
     * @param makerBuilder the metric maker builder
     * @param dimensionDictionary the dimension dictionary
     */
    public ConfiguredMetricLoader(
            MetricDictionary localDictionary,
            ConfigurationDictionary<MetricConfiguration> baseMetrics,
            ConfigurationDictionary<MetricConfiguration> derivedMetrics,
            MakerBuilder makerBuilder, DimensionDictionary dimensionDictionary
    ) {
        this.localDictionary = localDictionary;
        this.dimensionDictionary = dimensionDictionary;
        this.makerBuilder = makerBuilder;
        this.baseMetrics = baseMetrics;
        this.derivedMetrics = derivedMetrics;
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary) {
        localDictionary.clearLocal();

        // Note: will throw ConfigurationError if metric cannot be parsed
        for (Map.Entry<String, MetricConfiguration> entry : baseMetrics.entrySet()) {
            String name = entry.getKey();
            MetricConfiguration metric = entry.getValue();
            localDictionary
                    .add(metric.build(name, localDictionary, makerBuilder, dimensionDictionary));
        }

        for (Map.Entry<String, MetricConfiguration> entry : derivedMetrics.entrySet()) {
            String name = entry.getKey();
            MetricConfiguration metric = entry.getValue();
            localDictionary
                    .add(metric.build(name, localDictionary, makerBuilder, dimensionDictionary));
        }

        localDictionary.forEach((k, v) -> metricDictionary.add(v));
    }
}
