// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
            LinkedHashSet<Dimension> dimensions
    ) {
        this.tables = tables;
        this.apiMetricNames = apiMetricNames;
        this.dimensions = dimensions;
    }

    /**
     * Builds a TableGroup.
     * A TableGroup contains the dimensions, metrics, and backing physical tables intended to be attached to a
     * LogicalTable.
     * <p>
     * This constructor takes the union of the dimensions on the physical tables as the dimensions of the table
     * group.
     *
     * @param tables  The physical tables for the table group
     * @param apiMetricNames  The metrics for the table group
     *
     * @deprecated TableGroup should not be deriving its dimensions from physical tables, because a logical table may
     * only surface a subset of the dimensions on its PhysicalTable
     */
    @Deprecated
    public TableGroup(LinkedHashSet<PhysicalTable> tables, Set<ApiMetricName> apiMetricNames) {
        this(
                tables,
                apiMetricNames,
                tables.stream()
                        .flatMap(table -> table.getColumns(DimensionColumn.class).stream())
                        .map(DimensionColumn::getDimension)
                        .collect(Collectors.toCollection(LinkedHashSet::new))
        );
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
