// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import java.util.Comparator;

/**
 * Marker interface for objects that can be treated as a table name.
 */
public interface TableName {

    /**
     * Return a string representation of a table name.
     *
     * @return the name
     */
    String asName();

    /**
     * Wrap a string in an anonymous instance of TableName.
     * Rather than make heavy use of this, instead make a class.
     *
     * @param name the name being wrapped
     *
     * @return an anonymous subclass instance of TableName
     */
    static TableName of(String name) {
        return new TableName() {
            @Override
            public String asName() {
                return name;
            }

            @Override
            public int hashCode() {
                return name.hashCode();
            }

            @Override
            public boolean equals(Object o) {
                if (o != null && o instanceof TableName) {
                    return name.equals(((TableName) o).asName());
                }
                return false;
            }

            @Override
            public String toString() {
                return asName();
            }
        };
    }

    /**
     * Comparator to order TableNames by their asName methods, using the native String comparator.
     */
    Comparator<TableName> AS_NAME_COMPARATOR = Comparator.comparing(TableName::asName);

    /**
     * Comparator to order TableNames by their asName methods, using the native String comparator.
     *
     * @deprecated due to name change. Use AS_NAME_COMPARATOR instead.
     */
    @Deprecated
    Comparator<TableName> COMPARATOR = Comparator.comparing(TableName::asName);
}
