// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

/**
 * Marker interface for objects that can be treated as a table name in druid or web services.
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
     *
     * @param name the name being wrapped
     *
     * @return an anonymous subclass instance of ApiMetricName
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
                    return asName().equals(((TableName) o).asName());
                }
                return false;
            }
        };
    }
}
