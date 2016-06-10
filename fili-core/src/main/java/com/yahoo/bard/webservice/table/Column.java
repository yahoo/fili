// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

/**
 * Column
 */
public class Column {
    private final String name;

    /**
     * Constructor
     *
     * @param name  column name
     */
    public Column(String name) {
        this.name = name;
    }

    /**
     * Getter for column name
     *
     * @return name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Allows comparison based solely on the name between objects of the base class and/or any of the derived classes.
     *
     * @param o  The object to compare against.
     * @return True if equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null) { return false; }
        if (!(o instanceof Column)) { return false; }

        Column column = (Column) o;

        if (name != null ? !name.equals(column.name) : column.name != null) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
