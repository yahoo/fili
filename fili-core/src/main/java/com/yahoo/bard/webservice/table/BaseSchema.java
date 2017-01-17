// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * A parent class for most schema implementations.
 */
public class BaseSchema implements Schema {

    private final ImmutableSet<Column> columns;

    /**
     * Constructor.
     *
     * @param columns  The columns for this schema.
     */
    protected BaseSchema(Set<Column> columns) {
        this.columns = ImmutableSet.copyOf(columns);
    }

    @Override
    public Set<Column> getColumns() {
        return columns;
    }
}
