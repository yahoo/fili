// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric;

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private final ApiMetricName metricName;
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

    public MetricInstance(ApiMetricName metricName, MetricMaker maker, String... dependencyMetricNames) {
        this.metricName = metricName;
        this.maker = maker;
        this.dependencyMetricNames = Stream.of(dependencyMetricNames).collect(Collectors.toList());
    }


    /**
     * Construct a MetricInstance from FieldNames with a list of dependencyFields.
     * This method has an initial fieldName to disambiguate calls with empty lists.
     *
     * @param metricName  The name of the Logical Metric when it's in the metric dictionary
     * @param maker  The Metric Maker that creates the actual Logical Metric
     * @param firstField  The first fieldName instance (used to disambiguate from string array constructor
     * @param dependencyFields  The field names that this Logical Metric depends on
     */
    public MetricInstance(
            ApiMetricName metricName,
            MetricMaker maker,
            FieldName firstField,
            FieldName... dependencyFields
    ) {
        this(metricName, maker, toMetricNames(firstField, dependencyFields));
    }

    /**
     * A helper method for turning FieldNames into String metic names.
     *
     * @param firstField  The first fieldName in a list
     * @param fieldNames The rest of the fieldNames
     *
     * @return An array of the names from the field names
     */
    private static String[] toMetricNames(FieldName firstField, FieldName[] fieldNames) {
        return Stream.concat(Stream.of(firstField), Stream.of(fieldNames))
                .map(FieldName::asName)
                .toArray(String[]::new);
    }

    public ApiMetricName getMetricName() {
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
    public MetricInstance withName(ApiMetricName metricName) {
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
