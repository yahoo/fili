// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

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
    public boolean equals(Object object) {
        if (object == null || !(object instanceof TableNameImpl)) {
            return false;
        }

        TableNameImpl other = (TableNameImpl) object;
        return Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return 1 + Objects.hash(name);
    }
}
