// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.Schema;

import javax.validation.constraints.NotNull;

/**
 * Result DimensionColumn definition.
 */
public class DimensionColumn extends Column {

    private final Dimension dimension;

    /**
     * Constructor.
     * Uses dimension api name for column name.
     *
     * @param dimension  The column's corresponding dimension
     */
    protected DimensionColumn(@NotNull Dimension dimension) {
        this(dimension, dimension.getApiName());
    }

    /**
     * Constructor.
     * Uses the given name as  for column name.
     *
     * @param dimension  The column's corresponding dimension
     * @param columnName  Physical column name backing dimension
     */
    protected DimensionColumn(@NotNull Dimension dimension, @NotNull String columnName) {
        super(columnName);
        this.dimension = dimension;
    }

    public Dimension getDimension() {
        return this.dimension;
    }

    /**
     * Method to create a DimensionColumn tied to a schema.
     *
     * @param schema  The schema to which the column needs to be added
     * @param d  The dimension the column encapsulates
     *
     * @return The dimension column created
     */
    public static DimensionColumn addNewDimensionColumn(Schema schema, Dimension d) {
        DimensionColumn col = new DimensionColumn(d);
        schema.addColumn(col);
        return col;
    }

    /**
     * Method to create a DimensionColumn tied to a schema.
     *
     * @param schema  The schema to which the column needs to be added
     * @param d  The dimension the column encapsulates
     * @param physicalTable  Physical table associated with dimension column
     *
     * @return The dimension column created
     *
     * @deprecated in favor of addNewDimensionColumn(Schema, Dimension) which stores api name instead of physical name
     */
    @Deprecated
    public static DimensionColumn addNewDimensionColumn(Schema schema, Dimension d, PhysicalTable physicalTable) {
        DimensionColumn col = new DimensionColumn(d, physicalTable.getPhysicalColumnName(d.getApiName()));
        schema.addColumn(col);
        return col;
    }

    @Override
    public String toString() {
        return "{dim:'" + getName() + "'}";
    }
}
