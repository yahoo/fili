// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.druid.model.query.Granularity;

import java.util.Set;

/**
 * Everything you need to know about a physical table.
 */
public class PhysicalTableConfiguration {
    protected final String name;
    protected final Granularity granularity;
    protected final Set<String> dimensions;
    protected final Set<String> metrics;

    /**
     * Construct a new PhysicalTableConfiguration object.
     *
     * @param name the table name
     * @param granularity the table grain
     * @param dimensions the table dimensions
     * @param metrics the table metrics
     */
    public PhysicalTableConfiguration(String name, Granularity granularity, Set<String> dimensions, Set<String> metrics) {
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
}
