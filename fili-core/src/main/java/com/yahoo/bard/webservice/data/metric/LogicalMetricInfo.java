// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

/**
 * Contains information passed to LogicalMetric constructor.
 * The reason for having this class is to allow custom logical metric configurations.
 * For example, if only metric name is provided, other metric info, like description, are set to default, but if people
 * want to configure description, for example, they can add the description, which is going to be picked up during
 * logical metric construction.
 */
public class LogicalMetricInfo {
    private final String name;
    private final String longName;
    private final String category;
    private final String description;

    /**
     * Constructor. Builds a fully specified Logical Metric Info.
     *
     * @param name  Name of the metric
     * @param longName  Long name of the metric
     * @param category  Category of the metric
     * @param description  Description of the metric
     */
    public LogicalMetricInfo(String name, String longName, String category, String description) {
        this.name = name;
        this.longName = longName;
        this.category = category;
        this.description = description;
    }

    /**
     * Constructor.
     * Builds a partially specified Logical Metric Info.
     *
     * @param name  Name of the metric
     */
    public LogicalMetricInfo(String name) {
        this(name, name, LogicalMetric.DEFAULT_CATEGORY, name);
    }

    /**
     * Constructor.
     * Builds a partially specified Logical Metric Info.
     *
     * @param name  Name of the metric
     * @param longName  Long name of the metric
     */
    public LogicalMetricInfo(String name, String longName) {
        this(name, longName, LogicalMetric.DEFAULT_CATEGORY, name);
    }

    /**
     * Returns the name of the metric.
     *
     * @return the name of the metric
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the long name of the metric.
     *
     * @return the long name of the metric
     */
    public String getLongName() {
        return longName;
    }

    /**
     * Returns the category of the metric.
     *
     * @return the category of the metric
     */
    public String getCategory() {
        return category;
    }

    /**
     * Returns the description of the metric.
     *
     * @return the description of the metric
     */
    public String getDescription() {
        return description;
    }
}
