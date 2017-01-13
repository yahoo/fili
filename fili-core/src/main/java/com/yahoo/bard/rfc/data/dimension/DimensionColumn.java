// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.rfc.data.dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.table.Column;

import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * Result DimensionColumn definition.
 */
public class DimensionColumn extends Column {

    private final Dimension dimension;

    /**
     * Constructor.
     * Uses the given dimension's api name for column name.
     *
     * @param dimension  The column's corresponding dimension
     */
    public DimensionColumn(@NotNull Dimension dimension) {
        this(dimension, dimension.getApiName());
    }

    /**
     * Constructor.
     * Uses the given columnName for column name.
     *
     * @param dimension  The column's corresponding dimension
     * @param columnName  Column name backing dimension
     */
    protected DimensionColumn(@NotNull Dimension dimension, @NotNull String columnName) {
        super(columnName);
        this.dimension = dimension;
    }

    public Dimension getDimension() {
        return this.dimension;
    }

    @Override
    public int hashCode() {
        return dimension.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof DimensionColumn  && Objects.equals(((DimensionColumn) o).getDimension(), dimension));
    }

    @Override
    public String toString() {
        return "{dim:'" + getName() + "'}";
    }
}
