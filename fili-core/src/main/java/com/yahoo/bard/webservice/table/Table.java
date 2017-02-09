// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;

import java.util.LinkedHashSet;
import java.util.stream.Collectors;

/**
 * Table has a schema and a name.
 */
public interface Table {

    /**
     * The schema for this table.
     *
     * @return a schema
     */
    Schema getSchema();

    /**
     * The name for this table.
     *
     * @return The table name
     */
    String getName();

    /**
     * Getter for set of dimensions.
     *
     * @return Set of Dimension
     */
    default LinkedHashSet<Dimension> getDimensions() {
        return getSchema().getColumns(DimensionColumn.class).stream()
                .map(DimensionColumn::getDimension)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
