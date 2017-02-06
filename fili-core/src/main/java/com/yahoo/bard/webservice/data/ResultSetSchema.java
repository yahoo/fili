// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

import com.yahoo.bard.webservice.table.BaseSchema;
import com.yahoo.bard.webservice.table.GranularSchema;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.Column;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * The schema for a result set.
 */
public class ResultSetSchema extends BaseSchema implements GranularSchema {

    /**
     * The granularity of the ResultSet.
     */
    private Granularity granularity;

    /**
     * Constructor.
     *
     * @param granularity The bucketing time grain for this schema
     * @param columns The columns in this schema
     */
    public ResultSetSchema(
            @NotNull Granularity granularity, Set<Column> columns
    ) {
        super(columns);
        this.granularity = granularity;
    }

    /**
     * Copy constructor.
     *
     * @param resultSetSchema the result set schema being copied
     */
    public ResultSetSchema(ResultSetSchema resultSetSchema) {
        this(resultSetSchema.getGranularity(), new LinkedHashSet<>(resultSetSchema.getColumns()));
    }

    /**
     * Create a new result set with an additional final column.
     *
     * @param c the column being added
     *
     * @return the result set being constructed
     */
    public ResultSetSchema withAddColumn(Column c) {
        Set<Column> columns = new LinkedHashSet<>(this.getColumns());
        columns.add(c);
        return new ResultSetSchema(this.getGranularity(), columns);
    }

    @Override
    public Granularity getGranularity() {
        return granularity;
    }
}
