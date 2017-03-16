// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.metric.MetricInstance;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 *
 */
public class GenericMetricLoader implements MetricLoader {

    public static final int BYTES_PER_KILOBYTE = 1024;
    public static final int DEFAULT_KILOBYTES_PER_SKETCH = 16;
    public static final int DEFAULT_SKETCH_SIZE_IN_BYTES = DEFAULT_KILOBYTES_PER_SKETCH * BYTES_PER_KILOBYTE;
    private static final Logger LOG = LoggerFactory.getLogger(GenericMetricLoader.class);
    private final int sketchSize;
    private DoubleSumMaker doubleSumMaker;
    private Supplier<List<? extends DataSourceConfiguration>> configLoader;

    /**
     * Constructs a GenericMetricLoader using the default sketch size.
     */
    public GenericMetricLoader(Supplier<List<? extends DataSourceConfiguration>> configLoader) {
        this(DEFAULT_SKETCH_SIZE_IN_BYTES, configLoader);
    }

    /**
     * Constructs a GenericMetricLoader using the given sketch size.
     *
     * @param sketchSize  Sketch size, in number of bytes, to use for sketch operations
     * @param configLoader
     */
    public GenericMetricLoader(int sketchSize, Supplier<List<? extends DataSourceConfiguration>> configLoader) {
        this.sketchSize = sketchSize;
        this.configLoader = configLoader;
    }

    /**
     * (Re)Initialize the metric makers with the given metric dictionary.
     *
     * @param metricDictionary  Metric dictionary to use for generating the metric makers.
     */
    protected void buildMetricMakers(MetricDictionary metricDictionary) {
        // Create the various metric makers
        doubleSumMaker = new DoubleSumMaker(metricDictionary);
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary) {
        buildMetricMakers(metricDictionary);

        // Metrics that directly aggregate druid fields
        List<MetricInstance> metrics = new ArrayList<>();

        for (DataSourceConfiguration tableConfig : configLoader.get()) {
            for (String name : tableConfig.getMetrics()) {
                ApiMetricName apiMetricName = MetricNameGenerator.getFiliMetricName(
                        name,
                        tableConfig.getValidTimeGrains()
                );
                FieldName fieldName = MetricNameGenerator.getDruidMetric(
                        name
                );
                metrics.add(new MetricInstance(apiMetricName, doubleSumMaker, fieldName));
            }
        }

        LOG.debug("About to load direct aggregation metrics. Metric dictionary keys: {}", metricDictionary.keySet());
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
