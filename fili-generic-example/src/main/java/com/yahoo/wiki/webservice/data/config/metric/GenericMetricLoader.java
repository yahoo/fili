// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.metric.MetricInstance;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.util.Utils;
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
    private DoubleSumMaker doubleSumMaker;
    private final Supplier<List<? extends DataSourceConfiguration>> configLoader;

    /**
     * Constructs a GenericMetricLoader using the given sketch size.
     *
     * @param configLoader  Gives a list of {@link DataSourceConfiguration} to build the metrics from.
     */
    public GenericMetricLoader(@NotNull Supplier<List<? extends DataSourceConfiguration>> configLoader) {
        this.configLoader = configLoader;
    }

    /**
     * Initialize the metric makers with the given metric dictionary.
     *
     * @param metricDictionary  Metric dictionary to use for generating the metric makers.
     */
    protected void buildMetricMakers(MetricDictionary metricDictionary) {
        // Create the various metric makers
        if (doubleSumMaker == null) {
            doubleSumMaker = new DoubleSumMaker(metricDictionary);
        }
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary, DimensionDictionary dimensionDictionary) {
        buildMetricMakers(metricDictionary);

        // Metrics that directly aggregate druid fields
        List<MetricInstance> metrics = new ArrayList<>();

        configLoader.get()
                .forEach(dataSourceConfiguration -> {
                    dataSourceConfiguration.getMetrics()
                            .forEach(metric -> {
                                metrics.add(
                                        new MetricInstance(
                                                new LogicalMetricInfo(metric),
                                                doubleSumMaker,
                                                new DruidMetricName(metric)
                                        )
                                );
                            });
                });

        Utils.addToMetricDictionary(metricDictionary, metrics);
        LOG.debug("About to load direct aggregation metrics. Metric dictionary keys: {}", metricDictionary.keySet());
    }
}
