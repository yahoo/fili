// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.BaseSchema;
import com.yahoo.bard.webservice.table.Column;

import java.util.LinkedHashSet;

import javax.validation.constraints.NotNull;

/**
 * The schema for a result set.
 * The result set describes the set of data returned from a fact source.  It's schema includes dimension and metric
 * columns as well as a granularity describing how time is bucketed for the result set.
 */
public class ResultSetSchema extends BaseSchema {

    /**
     * Constructor.
     *
     * @param granularity The bucketing time grain for this schema
     * @param columns The columns in this schema
     */
    public ResultSetSchema(@NotNull Granularity granularity, Iterable<Column> columns) {
        super(granularity, columns);
    }

    /**
     * Create a new result set with an additional final column.
     *
     * @param column the column being added
     *
     * @return the result set being constructed
     */
    public ResultSetSchema withAddColumn(Column column) {
        LinkedHashSet<Column> columns = new LinkedHashSet<>(this.getColumns());
        columns.add(column);
        return new ResultSetSchema(this.getGranularity(), columns);
    }
}
