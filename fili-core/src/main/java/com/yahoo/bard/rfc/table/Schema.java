/*
 * // Copyright 2017 Yahoo Inc.
 * // Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
 *
 */
package com.yahoo.bard.rfc.table;

import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.util.Utils;

import java.util.Optional;
import java.util.Set;

public interface Schema extends Set<Column> {

    /**
     * Getter for set of columns by sub-type.
     *
     * @param columnClass  The class of columns to to search
     * @param <T> sub-type of Column to return
     *
     * @return Set of Columns
     */
    default <T extends Column> Set<T> getColumns(Class<T> columnClass) {
        return Utils.getSubsetByType(this, columnClass);
    }

    default <T extends Column> Optional<T> getColumn(String name, Class<T> columnClass) {
        return getColumns(columnClass).stream()
                .filter(column -> column.getName().equals(name))
                .findFirst();
    }
}
