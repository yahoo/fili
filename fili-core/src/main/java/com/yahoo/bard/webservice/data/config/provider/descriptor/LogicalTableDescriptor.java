// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.descriptor;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.Objects;
import java.util.Set;

/**
 * Everything you need to know to create a logical table.
 */
public class LogicalTableDescriptor {

    protected final String name;
    protected final Set<TimeGrain> timeGrains;
    protected final Set<String> physicalTables;
    protected final Set<String> metrics;

    /**
     * Construct a new logical table configuration object.
     *
     * @param name Logical table name
     * @param timeGrains Logical table time grains
     * @param physicalTables Physical tables backing this logical table
     * @param metrics Metrics included on this logical table
     */
    public LogicalTableDescriptor(
            String name,
            Set<TimeGrain> timeGrains,
            Set<String> physicalTables,
            Set<String> metrics) {
        this.name = name;
        this.timeGrains = timeGrains;
        this.physicalTables = physicalTables;
        this.metrics = metrics;
    }

    /**
     * Get the logical table name.
     *
     * @return the table name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the time grains for this logical table.
     *
     * FIXME: Not clear if this should deal in TimeGrains or Granularities.
     *
     * @return set of time grains
     */
    public Set<TimeGrain> getTimeGrains() {
        return timeGrains;
    }

    /**
     * Get the physical tables backing this logical table.
     *
     * @return set of physical table names
     */
    public Set<String> getPhysicalTables() {
        return physicalTables;
    }

    /**
     * Get the list of metrics this logical table provides.
     *
     * @return set of metric names
     */
    public Set<String> getMetrics() {
        return metrics;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        final LogicalTableDescriptor that = (LogicalTableDescriptor) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(timeGrains, that.timeGrains) &&
                Objects.equals(physicalTables, that.physicalTables) &&
                Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timeGrains, physicalTables, metrics);
    }
}
