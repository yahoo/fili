// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.descriptor;

import com.yahoo.bard.webservice.druid.model.query.Granularity;

import java.util.Objects;
import java.util.Set;

/**
 * Everything you need to know about a physical table.
 */
public class PhysicalTableDescriptor {

    protected final String name;
    protected final Granularity granularity;
    protected final Set<String> dimensions;
    protected final Set<String> metrics;

    /**
     * Construct a new PhysicalTableDescriptor object.
     *
     * @param name  the table name
     * @param granularity  the table grain
     * @param dimensions  the table dimensions
     * @param metrics  the table metrics
     */
    public PhysicalTableDescriptor(String name, Granularity granularity, Set<String> dimensions, Set<String> metrics) {
        this.name = name;
        this.granularity = granularity;
        this.dimensions = dimensions;
        this.metrics = metrics;
    }

    /**
     * Get the physical table name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the physical table granularity.
     *
     * @return the granularity
     */
    public Granularity getGranularity() {
        return granularity;
    }

    /**
     * Get the dimensions of the physical table.
     *
     * @return the dimensions
     */
    public Set<String> getDimensions() {
        return dimensions;
    }

    /**
     * Get the metrics of the physical table.
     *
     * @return the metrics
     */
    public Set<String> getMetrics() {
        return metrics;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        final PhysicalTableDescriptor that = (PhysicalTableDescriptor) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(granularity, that.granularity) &&
                Objects.equals(dimensions, that.dimensions) &&
                Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, granularity, dimensions, metrics);
    }
}
