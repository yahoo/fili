// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Metric instance holds all of the information needed to construct a LogicalMetric.
 * <dl>
 *     <dt>Metric name
 *     <dd>The name of the Logical Metric when it's in the metric dictionary
 *     <dt>Dependency Metric Names
 *     <dd>The names of metrics either in the dictionary or raw druid metrics that this Logical Metric depends on
 *     <dt>Maker
 *     <dd>The Metric Maker that creates the actual Logical Metric
 * </dl>
 */
public class MetricInstance {

    private final String metricName;
    private final List<String> dependencyMetricNames;
    private final MetricMaker maker;

    /**
     * Construct a MetricInstance from Strings with a list of dependencyMetricNames.
     *
     * @param metricName  The name of the Logical Metric in the metric dictionary
     * @param maker  The Metric Maker that creates the actual Logical Metric
     * @param dependencyMetricNames  The names of metrics either in the dictionary or raw druid metrics that this
     * Logical Metric depends on
     */
    public MetricInstance(String metricName, MetricMaker maker, String... dependencyMetricNames) {
        this.metricName = metricName;
        this.maker = maker;
        this.dependencyMetricNames = Arrays.asList(dependencyMetricNames);
    }

    /**
     * Construct a MetricInstance from FieldNames with a list of dependencyFields.
     *
     * @param metricName  The name of the Logical Metric when it's in the metric dictionary
     * @param maker  The Metric Maker that creates the actual Logical Metric
     * @param dependencyFields  The field names that this Logical Metric depends on
     */
    public MetricInstance(FieldName metricName, MetricMaker maker, FieldName... dependencyFields) {
        this.metricName = metricName.asName();
        this.maker = maker;
        this.dependencyMetricNames = new ArrayList<>();
        for (FieldName fieldName : dependencyFields) {
            this.dependencyMetricNames.add(fieldName.asName());
        }
    }

    public String getMetricName() {
        return metricName;
    }

    public List<String> getDependencyMetricNames() {
        return dependencyMetricNames;
    }

    public MetricMaker getMaker() {
        return maker;
    }

    /**
     * Makes a copy of the metric instance with new metric name.
     *
     * @param metricName The name of the Logical Metric in the metric dictionary
     * @return copy of the logical Metric
     */
    public MetricInstance withName(String metricName) {
        return new MetricInstance(
                metricName,
                maker,
                dependencyMetricNames.toArray(new String[dependencyMetricNames.size()])
        );
    }

    /**
     * Makes a copy of the metric instance with new maker.
     *
     * @param maker The Metric Maker that creates the actual Logical Metric
     * @return copy of the logical Metric
     */
    public MetricInstance withMaker(MetricMaker maker) {
        return new MetricInstance(
                metricName,
                maker,
                dependencyMetricNames.toArray(new String[dependencyMetricNames.size()])
        );
    }

    /**
     * Makes a copy of the metric instance with new dependencyMetricNames.
     *
     * @param dependencyMetricNames The name of metrics that this Logical Metric depends on
     * @return copy of the logical Metric
     */
    public MetricInstance withDependencyMetricNames(List<String> dependencyMetricNames) {
        return new MetricInstance(
                metricName,
                maker,
                dependencyMetricNames.toArray(new String[dependencyMetricNames.size()])
        );
    }

    /**
     * Make the Logical Metric from the information in this MetricInstance.
     *
     * @return The LogicalMetric with the provided name, using the given maker, that depends on the given metrics.
     */
    public LogicalMetric make() {
        return maker.make(metricName, dependencyMetricNames);
    }
}
