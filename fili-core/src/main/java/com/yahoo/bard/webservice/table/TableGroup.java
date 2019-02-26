// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A TableGroup is a list of schemas.
 */
public class TableGroup {

    private final LinkedHashSet<Dimension> dimensions;
    private final LinkedHashSet<PhysicalTable> tables;
    private final Set<ApiMetricName> apiMetricNames;

    /**
     * Builds a TableGroup.
     * A TableGroup contains the dimensions, metrics, and backing physical tables intended to be attached to a
     * LogicalTable.
     *
     * @param tables  The backing physical tables
     * @param apiMetricNames  The metric names for a LogicalTable
     * @param dimensions  The dimensions for a LogicalTable
     */
    public TableGroup(
            LinkedHashSet<PhysicalTable> tables,
            Set<ApiMetricName> apiMetricNames,
            Set<Dimension> dimensions
    ) {
        this.tables = tables;
        this.apiMetricNames = apiMetricNames;
        this.dimensions = new LinkedHashSet<>(dimensions);
    }

    /**
     * Getter for set of physical tables.
     *
     * @return physicalTableSchema
     */
    public Set<PhysicalTable> getPhysicalTables() {
        return this.tables;
    }

    /**
     * Getter for the set of maximal dimensions for tables in this table group.
     *
     * @return dimensions
     */
    public Set<Dimension> getDimensions() {
        return dimensions;
    }

    @Override
    public String toString() {
        return "TableGroup: " + tables ;
    }

    /**
     * Getter for the api metric names on this group.
     *
     * @return api metric names
     */
    public Set<ApiMetricName> getApiMetricNames() {
        return apiMetricNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        TableGroup that = (TableGroup) o;
        return
                Objects.equals(dimensions, that.dimensions) &&
                Objects.equals(tables, that.tables) &&
                Objects.equals(apiMetricNames, that.apiMetricNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimensions, tables, apiMetricNames);
    }
}
