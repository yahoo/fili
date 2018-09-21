// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import java.util.Objects;

/**
 * Contains information passed to LogicalMetric constructor.
 * The reason for having this class is to allow custom logical metric configurations.
 * For example, if only metric name is provided, other metric info, like description, are set to default, but if people
 * want to configure description, for example, they can add the description, which is going to be picked up during
 * logical metric construction.
 */
public class LogicalMetricInfo {
    private static final String TYPE_DEFAULT = "number";

    private final String name;
    private final String longName;
    private final String category;
    private final String description;
    private final String type;

    /**
     * Constructor. Builds a fully specified Logical Metric Info.
     *
     * @param name  Name of the metric
     * @param longName  Long name of the metric
     * @param category  Category of the metric
     * @param description  Description of the metric
     * @param type  Type of metric
     */
    public LogicalMetricInfo(String name, String longName, String category, String description, String type) {
        this.name = name;
        this.longName = longName;
        this.category = category;
        this.description = description;
        this.type = type;
    }

    /**
     * Constructor. Builds a fully specified Logical Metric Info.
     *
     * @param name  Name of the metric
     * @param longName  Long name of the metric
     * @param category  Category of the metric
     * @param description  Description of the metric
     */
    public LogicalMetricInfo(String name, String longName, String category, String description) {
        this(name, longName, category, description, TYPE_DEFAULT);
    }

    /**
     * Constructor.
     * Builds a partially specified Logical Metric Info.
     *
     * @param name  Name of the metric
     */
    public LogicalMetricInfo(String name) {
        this(name, name, LogicalMetric.DEFAULT_CATEGORY, name, TYPE_DEFAULT);
    }

    /**
     * Constructor.
     * Builds a partially specified Logical Metric Info.
     *
     * @param name  Name of the metric
     * @param longName  Long name of the metric
     */
    public LogicalMetricInfo(String name, String longName) {
        this(name, longName, LogicalMetric.DEFAULT_CATEGORY, name, TYPE_DEFAULT);
    }

    /**
     * Constructor.
     * Builds a partially specified Logical Metric Info.
     *
     * @param name  Name of the metric
     * @param longName  Long name of the metric
     * @param description  Description for the metric
     */
    public LogicalMetricInfo(String name, String longName, String description) {
        this(name, longName, LogicalMetric.DEFAULT_CATEGORY, description, TYPE_DEFAULT);
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

    /**
     * Returns the type of the metric.
     *
     * @return the type of the metric
     */
    public String getType() {
        return type;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        LogicalMetricInfo that = (LogicalMetricInfo) o;

        return Objects.equals(name, that.name) &&
                Objects.equals(longName, that.longName) &&
                Objects.equals(category, that.category) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, longName, category, description);
    }
}
