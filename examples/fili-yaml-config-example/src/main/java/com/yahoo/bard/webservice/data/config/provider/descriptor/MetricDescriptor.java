// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.descriptor;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.Objects;
import java.util.Set;

/**
 * Everything you need to know about a metric.
 */
public class MetricDescriptor {

    protected final String name;
    protected final String longName;
    protected final String category;
    protected final String description;
    protected final String definition;
    protected final boolean isExcluded;
    protected final Set<TimeGrain> validGrains;

    /**
     * Construct a new metric configuration object.
     *
     * @param name  The metric name
     * @param longName  The long metric name
     * @param category  The metric category
     * @param description  The metric description
     * @param definition  The metric definition
     * @param isExcluded  True to exclude the metric from the final metric dictionary
     * @param validGrains  Valid time grains for this metric
     */
    public MetricDescriptor(
            String name,
            String longName,
            String category,
            String description,
            String definition,
            boolean isExcluded,
            Set<TimeGrain> validGrains) {
        this.name = name;
        this.longName = longName;
        this.category = category;
        this.description = description;
        this.definition = definition;
        this.isExcluded = isExcluded;
        this.validGrains = validGrains;
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

    /**
     * Get the time grains this metric is valid for.
     *
     * @return set of time grains
     */
    public Set<TimeGrain> getValidGrains() {
        return validGrains;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        final MetricDescriptor that = (MetricDescriptor) o;
        return isExcluded == that.isExcluded &&
                Objects.equals(name, that.name) &&
                Objects.equals(longName, that.longName) &&
                Objects.equals(category, that.category) &&
                Objects.equals(description, that.description) &&
                Objects.equals(validGrains, that.validGrains) &&
                Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, longName, category, description, definition, isExcluded, validGrains);
    }
}
