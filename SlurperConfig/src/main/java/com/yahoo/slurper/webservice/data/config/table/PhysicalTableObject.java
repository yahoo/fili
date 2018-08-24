// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.data.config.table;

import com.yahoo.slurper.webservice.data.config.JsonObject;

import javax.validation.constraints.NotNull;
import java.util.Locale;
import java.util.Set;

/**
 * Physical table object for parsing to json.
 */
public class PhysicalTableObject extends JsonObject {

    private final String name;
    private final String granularity;
    private final Set<String> dimensions;
    private final Set<String> metrics;

    /**
     * Construct a physical table config instance from data source config.
     *
     * @param name The the name of the data source.
     * @param granularity The granularity for the data source.
     * @param dimensions The dimensions for the data source.
     * @param metrics The metrics for the data source.
     */
    public PhysicalTableObject(
            @NotNull String name,
            @NotNull String granularity,
            @NotNull Set<String> dimensions,
            @NotNull Set<String> metrics
    )  {
        this.name = name;
        this.granularity = granularity.toUpperCase(Locale.ENGLISH);
        this.dimensions = dimensions;
        this.metrics = metrics;
    }

    /**
     * Gets the name of the table.
     *
     * @return the name of the table
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the granularity for the table.
     *
     * @return the granularity for the table
     */
    public String getGranularity() {
        return granularity;
    }

    /**
     * Gets the dimensions from the datasource.
     *
     * @return the names of the dimensions for the datasource
     */
    public Set<String> getDimensions() {
        return dimensions;
    }

    /**
     * Gets the metrics from the datasource.
     *
     * @return the names of metrics for the datasource
     */
    public Set<String> getMetrics() {
        return metrics;
    }
}
