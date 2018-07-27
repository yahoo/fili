// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.data.config.table;

import com.yahoo.slurper.webservice.data.config.JsonObject;

import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Logical table object for parsing to json.
 */
public class LogicalTableObject extends JsonObject {

    private final String name;
    private final Set<String> granularity = new HashSet<>();
    private final Set<String> dimensions;
    private final Set<String> apiMetricNames;
    private final Set<String> physicalTables = new HashSet<>();

    /**
     * Construct a logical table config instance from data source config.
     *
     * @param name The the name of the data source.
     * @param granularity The granularity for the logical table.
     * @param dimensions The dimensions for the logical table.
     * @param metrics The metrics for the data logical table.
     * @param physicalTable The physical tables this logical table depends on.
     */
    public LogicalTableObject(
            @NotNull String name,
            @NotNull String granularity,
            @NotNull Set<String> dimensions,
            @NotNull Set<String> metrics,
            @NotNull String physicalTable
    )  {
        this.name = name;
        this.granularity.add(granularity.toUpperCase(Locale.ENGLISH));
        this.granularity.add("ALL");
        this.dimensions = dimensions;
        this.apiMetricNames = metrics;
        this.physicalTables.add(physicalTable);
    }

    /**
     * Gets the name of the logical table.
     *
     * @return the name of the logical table
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the granularity for the logical table.
     *
     * @return the granularity for the logical table
     */
    public Set<String> getGranularity() {
        return granularity;
    }

    /**
     * Gets the dimensions from the logical table.
     *
     * @return the names of the dimensions for the logical table
     */
    public Set<String> getDimensions() {
        return dimensions;
    }

    /**
     * Gets the metrics from the logical table.
     *
     * @return the names of metrics for the logical table
     */
    public Set<String> getApiMetricNames() {
        return apiMetricNames;
    }

    /**
     * Gets the physical tables this logical table depends on.
     *
     * @return the physical tables this logical table depends on
     */
    public Set<String> getPhysicalTables() {
        return physicalTables;
    }
}
