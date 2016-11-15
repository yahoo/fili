// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.io.IOException;
import java.util.Map;

/**
 * A config-driven MetricLoader.
 */
public class ConfiguredMetricLoader implements MetricLoader {

    private final MetricDictionary localDictionary;
    private final MetricDictionary tempDictionary;
    private final MakerDictionary makerDictionary;
    private final DimensionDictionary dimensionDictionary;
    private final ConfigurationDictionary<MetricConfiguration> baseMetrics;
    private final ConfigurationDictionary<MetricConfiguration> derivedMetrics;

    /**
     * Construct a new MetricLoader with the given configuration.
     *
     * @param localDictionary the local metric dictionary
     * @param tempDictionary the temporary metric dictionary
     * @param baseMetrics the configured base metrics
     * @param derivedMetrics the configured derived metrics
     * @param makerDictionary the metric maker dictionary
     * @param dimensionDictionary the dimension dictionary
     */
    public ConfiguredMetricLoader(
            MetricDictionary localDictionary, MetricDictionary tempDictionary,
            ConfigurationDictionary<MetricConfiguration> baseMetrics,
            ConfigurationDictionary<MetricConfiguration> derivedMetrics,
            MakerDictionary makerDictionary, DimensionDictionary dimensionDictionary
    ) {
        this.localDictionary = localDictionary;
        this.tempDictionary = tempDictionary;
        this.dimensionDictionary = dimensionDictionary;
        this.makerDictionary = makerDictionary;
        this.baseMetrics = baseMetrics;
        this.derivedMetrics = derivedMetrics;
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary) {
        localDictionary.clearLocal();

        try {
            for (Map.Entry<String, MetricConfiguration> entry : baseMetrics.entrySet()) {
                String name = entry.getKey();
                MetricConfiguration metric = entry.getValue();
                localDictionary
                        .add(metric.build(name, localDictionary, tempDictionary, makerDictionary, dimensionDictionary));
                tempDictionary.clearLocal();
            }

            for (Map.Entry<String, MetricConfiguration> entry : derivedMetrics.entrySet()) {
                String name = entry.getKey();
                MetricConfiguration metric = entry.getValue();
                localDictionary
                        .add(metric.build(name, localDictionary, tempDictionary, makerDictionary, dimensionDictionary));
                tempDictionary.clearLocal();
            }
        } catch (IOException ex) {
            // this needs to be cleaned up
            throw new RuntimeException(ex);
        }

        localDictionary.forEach((k, v) -> metricDictionary.add(v));
    }
}
