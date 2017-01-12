// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.rfc.table;

import com.yahoo.bard.webservice.table.Column;

import java.util.Objects;

/**
 * Metric.
 */
public class MetricColumn extends Column {

    /**
     * Constructor.
     *
     * @param name  The column name
     */
    protected MetricColumn(String name) {
        super(name);
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof MetricColumn && Objects.equals(getName(), ((MetricColumn) o).getName()));
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "{metric:'" + getName() + "'}";
    }
}
