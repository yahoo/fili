// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.metric.MetricInstance;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import javax.validation.constraints.NotNull;

/**
 * Loads all metrics from a list of {@link DataSourceConfiguration}.
 */
public class GenericMetricLoader implements MetricLoader {

    private static final Logger LOG = LoggerFactory.getLogger(GenericMetricLoader.class);
    private final Supplier<List<? extends DataSourceConfiguration>> configLoader;

    /**
     * Constructs a GenericMetricLoader using the given sketch size.
     *
     * @param configLoader  Gives a list of {@link DataSourceConfiguration} to build the metrics from.
     */
    public GenericMetricLoader(
            @NotNull Supplier<List<? extends DataSourceConfiguration>> configLoader
    ) {
        this.configLoader = configLoader;
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary) {
        // Metrics that directly aggregate druid fields
        List<MetricInstance> metrics = new ArrayList<>();

        configLoader.get()
                .forEach(dataSourceConfiguration -> {
                    dataSourceConfiguration.getMetricConfigs()
                            .stream()
                            .map(metricConfig -> metricConfig.getMetricInstance(metricDictionary))
                            .forEach(metrics::add);
                });

        LOG.debug("About to load direct aggregation metrics. Metric dictionary keys: {}", metricDictionary.keySet());
        metrics.sort((firstMetric, secondMetric) -> {
                    if (firstMetric.getDependencyMetricNames().contains(secondMetric.getMetricName())) {
                        return 1;
                    } else if (secondMetric.getDependencyMetricNames().contains(firstMetric.getMetricName())) {
                        return -1;
                    } else {
                        return Integer.compare(
                                firstMetric.getDependencyMetricNames().size(),
                                secondMetric.getDependencyMetricNames().size()
                        );
                    }
                }
        );
        addToMetricDictionary(metricDictionary, metrics);
    }

    /**
     * Create metrics from instance descriptors and store in the metric dictionary.
     *
     * @param metricDictionary  The dictionary to store metrics in
     * @param metrics  The list of metric descriptors
     */
    private void addToMetricDictionary(MetricDictionary metricDictionary, List<MetricInstance> metrics) {
        metrics.stream().map(MetricInstance::make).forEach(metricDictionary::add);
    }
}
