// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.metric.MetricInstance;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.metric.makers.CountMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.DoubleSumMaker;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.wiki.webservice.data.config.names.WikiApiMetricName;
import com.yahoo.wiki.webservice.data.config.names.WikiDruidMetricName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Load the Wiki-specific metrics. Currently only loads primitive makers that are considered built-in to Fili,
 * such as the LongSumMaker for performing the longSum aggregation, and the divisionMaker, which performs division
 * of two other metrics.
 */
public class WikiMetricLoader implements MetricLoader {

    private static final Logger LOG = LoggerFactory.getLogger(WikiMetricLoader.class);

    public static final int BYTES_PER_KILOBYTE = 1024;
    public static final int DEFAULT_KILOBYTES_PER_SKETCH = 16;
    public static final int DEFAULT_SKETCH_SIZE_IN_BYTES = DEFAULT_KILOBYTES_PER_SKETCH * BYTES_PER_KILOBYTE;

    final int sketchSize;

    CountMaker countMaker;
    DoubleSumMaker doubleSumMaker;

    /**
     * Constructs a WikiMetricLoader using the default sketch size.
     */
    public WikiMetricLoader() {
        this(DEFAULT_SKETCH_SIZE_IN_BYTES);
    }

    /**
     * Constructs a WikiMetricLoader using the given sketch size.
     *
     * @param sketchSize  Sketch size, in number of bytes, to use for sketch operations
     */
    public WikiMetricLoader(int sketchSize) {
        this.sketchSize = sketchSize;
    }

    /**
     * (Re)Initialize the metric makers with the given metric dictionary.
     *
     * @param metricDictionary  Metric dictionary to use for generating the metric makers.
     */
    protected void buildMetricMakers(MetricDictionary metricDictionary) {
        // Create the various metric makers
        countMaker = new CountMaker(metricDictionary);
        doubleSumMaker = new DoubleSumMaker(metricDictionary);
    }

    @Override
    public void loadMetricDictionary(MetricDictionary metricDictionary) {
        buildMetricMakers(metricDictionary);

        // Metrics that directly aggregate druid fields
        List<MetricInstance> metrics;
        metrics = Arrays.asList(
                new MetricInstance(WikiApiMetricName.COUNT, countMaker),
                new MetricInstance(WikiApiMetricName.ADDED, doubleSumMaker, WikiDruidMetricName.ADDED),
                new MetricInstance(WikiApiMetricName.DELETED, doubleSumMaker, WikiDruidMetricName.DELETED),
                new MetricInstance(WikiApiMetricName.DELTA, doubleSumMaker, WikiDruidMetricName.DELTA)
        );
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
