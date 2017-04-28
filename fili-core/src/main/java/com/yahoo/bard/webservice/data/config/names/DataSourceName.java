// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import java.util.Comparator;

/**
 * Marker interface for objects that can be treated as a data source name in druid.
 */
public interface DataSourceName {

    /**
     * Comparator to order DataSourceNames by their asName methods, using the native String comparator.
     */
    Comparator<DataSourceName> AS_NAME_COMPARATOR = Comparator.comparing(DataSourceName::asName);

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
     * @param name   The name being wrapped
     *
     * @return an anonymous subclass instance of TableName
     */
    static DataSourceName of(String name) {
        return new DataSourceName() {
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
                if (o != null && o instanceof DataSourceName) {
                    return name.equals(((DataSourceName) o).asName());
                }
                return false;
            }

            @Override
            public String toString() {
                return asName();
            }
        };
    }
}
