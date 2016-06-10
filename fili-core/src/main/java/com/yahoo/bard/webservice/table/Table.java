// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Table is a schema with dimension columns
 */
public class Table extends Schema {

    private final String name;

    /**
     * Constructor
     *
     * @param name  The name of the table
     * @param granularity  The granularity of the table
     */
    public Table(@NotNull String name, @NotNull Granularity granularity) {
        super(granularity);
        this.name = name;
    }

    /**
     * Getter for set of dimensions
     *
     * @return Set of Dimension
     */
    public Set<Dimension> getDimensions() {
        return this.getColumns(DimensionColumn.class).stream()
                .map(DimensionColumn::getDimension)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "{Table:{ name:'" + getName() + "', grain:'" + getGranularity() + "', cols:'" + getColumns() + "'} }";
    }

    @Override
    public boolean equals(final Object o) {
        return super.equals(o) && name.equals(((Table) o).name);
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + getName().hashCode();
    }
}
