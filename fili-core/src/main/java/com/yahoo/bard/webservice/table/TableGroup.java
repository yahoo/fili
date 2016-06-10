// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;

import java.util.LinkedHashSet;
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
     * Constructor
     * @param tables  The physical tables for the table group
     * @param apiMetricNames  The metrics for the table group
     */
    public TableGroup(LinkedHashSet<PhysicalTable> tables, Set<ApiMetricName> apiMetricNames) {
        this.tables = tables;
        this.apiMetricNames = apiMetricNames;
        this.dimensions = tables.stream()
                .flatMap(table -> table.getColumns(DimensionColumn.class).stream())
                .map(DimensionColumn::getDimension)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Getter for set of physical tables
     *
     * @return physicalTableSchema
     */
    public Set<PhysicalTable> getPhysicalTables() {
        return this.tables;
    }

    /**
     * Getter for the set of maximal dimensions for tables in this table group
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
     * Getter for the api metric names on this group
     *
     * @return api metric names
     */
    public Set<ApiMetricName> getApiMetricNames() {
        return apiMetricNames;
    }

    //CHECKSTYLE:OFF
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableGroup that = (TableGroup) o;

        if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) return false;
        if (tables != null ? !tables.equals(that.tables) : that.tables != null) return false;
        return !(apiMetricNames != null ? !apiMetricNames.equals(that.apiMetricNames) : that.apiMetricNames != null);

    }

    @Override
    public int hashCode() {
        int result = dimensions != null ? dimensions.hashCode() : 0;
        result = 31 * result + (tables != null ? tables.hashCode() : 0);
        result = 31 * result + (apiMetricNames != null ? apiMetricNames.hashCode() : 0);
        return result;
    }
    //CHECKSTYLE:ON
}
