// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.impl;

import com.yahoo.bard.webservice.data.config.names.TableName;

import java.util.Objects;

/**
 * Table name.
 */
public class TableNameImpl implements TableName {

    protected final String name;

    /**
     * Construct a new table name.
     *
     * @param name the table name
     */
    public TableNameImpl(String name) {
        this.name = name;
    }

    @Override
    public String asName() {
        return name;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        final TableNameImpl tableName = (TableNameImpl) o;
        return Objects.equals(name, tableName.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
