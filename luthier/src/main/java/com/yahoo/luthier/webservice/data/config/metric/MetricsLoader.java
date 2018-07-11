// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.data.config.metric.MetricInstance;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Load the Wiki-specific metrics. Currently only loads primitive makers that are considered built-in to Fili,
 * such as the LongSumMaker for performing the longSum aggregation, and the divisionMaker, which performs division
 * of two other metrics.
 */
public class MetricsLoader implements MetricLoader {

    private static final Logger LOG = LoggerFactory.getLogger(MetricLoader.class);

    private static MetricMakerDictionary metricMakerDictionary;
    private ExternalConfigLoader metricConfigLoader;
    private String externalConfigFilePath;

    /**
     * Constructor using the default external configuration loader
     * and default external configuration file path.
     */
    public MetricsLoader() {
        this(new ExternalConfigLoader(new ObjectMapper()),
                "config/");
    }

    /**
     * Constructor using the default external configuration file path.
     *
     * @param externalConfigFilePath The external file's url containing the external config information
     */
    public MetricsLoader(String externalConfigFilePath) {
        this(new ExternalConfigLoader(new ObjectMapper()),
                externalConfigFilePath);
    }

    /**
     * Constructs a MetricLoader.
     *
     * @param metricConfigLoader The external configuration loader for loading metrics
     * @param externalConfigFilePath The external file's url containing the external config information
     */
    public MetricsLoader(ExternalConfigLoader metricConfigLoader, String externalConfigFilePath) {
        this.metricConfigLoader = metricConfigLoader;
        this.externalConfigFilePath = externalConfigFilePath;
    }

    /**
     * (Re)Initialize the Metric Makers dictionary.
     *
     * @param metricMakers        The configuration of metric makers
     * @param metricDictionary    The dictionary that will be loaded with metrics
     * @param dimensionDictionary The dimension dictionary containing loaded dimensions
     */
    protected void buildMetricMakersDictionary(LinkedHashSet<MetricMakerTemplate> metricMakers,
                                               MetricDictionary metricDictionary,
                                               DimensionDictionary dimensionDictionary) {
        metricMakerDictionary = new MetricMakerDictionary(metricMakers, metricDictionary, dimensionDictionary);
    }

    /**
     * Select and return a Metric Maker by it's name.
     *
     * @param metricMakerName Metric Maker's name
     * @return a specific Metric Maker Instance
     */
    protected MetricMaker selectMetricMakersByName(String metricMakerName) {
        return metricMakerDictionary.findByName(metricMakerName);
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary, DimensionDictionary dimensionDictionary) {

        MetricConfigTemplate metricConfig =
                metricConfigLoader.parseExternalFile(externalConfigFilePath + "MetricConfig.json",
                        MetricConfigTemplate.class
                );

        buildMetricMakersDictionary(metricConfig.getMakers(), metricDictionary, dimensionDictionary);

        List<MetricInstance> metrics = metricConfig.getMetrics().stream().map(
                metric -> new MetricInstance(
                        new LogicalMetricInfo(metric.asName(), metric.getLongName(), metric.getDescription()),
                        selectMetricMakersByName(metric.getMakerName()),
                        metric.getDependencyMetricNames()
                )
        ).collect(Collectors.toList());

        Utils.addToMetricDictionary(metricDictionary, metrics);
        LOG.debug("About to load direct aggregation metrics. Metric dictionary keys: {}", metricDictionary.keySet());
    }
}
