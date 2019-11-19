// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.table.Column;

import javax.validation.constraints.NotNull;

/**
 * Result DimensionColumn definition.
 */
public class DimensionColumn extends Column {

    private final Dimension dimension;

    /**
     * Constructor.
     * Uses the given dimension's name for column name.
     *
     * @param dimension  The column's corresponding dimension
     */
    public DimensionColumn(@NotNull Dimension dimension) {
        super(dimension.getApiName());
        this.dimension = dimension;
    }

    public Dimension getDimension() {
        return this.dimension;
    }

    @Override
    public String toString() {
        return "{dim:'" + getName() + "'}";
    }
}
