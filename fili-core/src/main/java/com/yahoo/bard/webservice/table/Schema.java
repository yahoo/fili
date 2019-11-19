// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.util.Utils;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * An interface describing a table or table-like entity composed of sets of columns.
 */
public interface Schema {

    /**
     * Get all the columns underlying this Schema.
     *
     * @return The columns of this schema
     */
    Set<Column> getColumns();

    /**
     * Get the time granularity for this Schema.
     *
     * @return The time granularity of this schema
     */
    Granularity getGranularity();

    /**
     * Getter for set of columns by sub-type.
     *
     * @param columnClass  The class of columns to to search
     * @param <T> sub-type of Column to return
     *
     * @return Set of Columns
     */
    default <T extends Column> LinkedHashSet<T> getColumns(Class<T> columnClass) {
        return Utils.getSubsetByType(getColumns(), columnClass);
    }

    /**
     * Given a column type and name, return the column of the expected type.
     *
     * @param name  The name on the column
     * @param columnClass The class of the column being retrieved
     * @param <T> The type of the subclass of the column being retrieved
     *
     * @return  The an optional containing the column of the name and type specified, if any
     */
    default <T extends Column> Optional<T> getColumn(String name, Class<T> columnClass) {
        return getColumns(columnClass).stream()
                .filter(column -> column.getName().equals(name))
                .findFirst();
    }
}
