// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

/**
 * Everything you need to know about a metric.
 */
public class MetricConfiguration {

    protected final String name;
    protected final String longName;
    protected final String category;
    protected final String description;
    protected final String definition;
    protected final boolean isExcluded;

    /**
     * Construct a new metric configuration object.
     *
     * @param name The metric name
     * @param longName The long metric name
     * @param category The metric category
     * @param description The metric description
     * @param definition The metric definition
     * @param isExcluded True to exclude the metric from the final metric dictionary
     */
    public MetricConfiguration(String name, String longName, String category, String description, String definition, boolean isExcluded) {
        this.name = name;
        this.longName = longName;
        this.category = category;
        this.description = description;
        this.definition = definition;
        this.isExcluded = isExcluded;
    }

    /**
     * Get the name of the metric.
     *
     * @return the name.
     */
    public String getName() {
        return name;
    }

    /**
     * Get the long name of the metric.
     *
     * @return the long name.
     */
    public String getLongName() {
        return longName;
    }

    /**
     * Get the category of the metric.
     *
     * @return the metric category
     */
    public String getCategory() {
        return category;
    }

    /**
     * Get the description of the metric.
     *
     * @return the metric description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the definition of the metric.
     *
     * Useful if you'd like to do something like parse a metric from an expression-language definition.
     *
     * @return the metric definition
     */
    public String getDefinition() {
        return definition;
    }

    /**
     * Return true if the metric should be excluded from the metric dictionary.
     *
     * @return true if excluded, false otherwise
     */
    public boolean isExcluded() {
        return isExcluded;
    }
}
