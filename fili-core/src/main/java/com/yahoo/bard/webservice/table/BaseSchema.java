// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.druid.model.query.Granularity;

import com.google.common.collect.Sets;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;

/**
 * A parent class for most schema implementations.
 */
public class BaseSchema implements Schema {

    private final LinkedHashSet<Column> columns;
    private final Granularity granularity;

    /**
     * Constructor.
     *
     * @param granularity  The granularity for this schema.
     * @param columns  The columns for this schema.
     */
    protected BaseSchema(Granularity granularity, Iterable<Column> columns) {
        this.granularity = granularity;
        this.columns = Sets.newLinkedHashSet(columns);
    }

    @Override
    public Optional<Column> getColumn(String columnName) {
        return getColumns().stream()
                .filter(column -> column.getName().equals(columnName))
                .findFirst();
    }

    @Override
    public LinkedHashSet<Column> getColumns() {
        return columns;
    }

    @Override
    public Granularity getGranularity() {
        return granularity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseSchema)) {
            return false;
        }

        BaseSchema that = (BaseSchema) o;
        return this.getClass() == o.getClass()
                && Objects.equals(columns, that.columns)
                && Objects.equals(granularity, that.getGranularity()
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(granularity, columns);
    }
}
