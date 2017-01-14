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

public class ResultSetSchema extends BaseSchema implements GranularSchema {

    private Granularity granularity;

    /**
     *
     * @param columns The columns in this schema
     * @param granularity The bucketing time grain for this schema
     */
    public ResultSetSchema(
            Set<Column> columns,
            @NotNull Granularity granularity
    ) {
        super(columns);
        this.granularity = granularity;
    }

    public ResultSetSchema(ResultSetSchema resultSetSchema) {
        this(resultSetSchema.getColumns(), resultSetSchema.getGranularity());
    }

    public ResultSetSchema withAddColumn(Column c) {
        Set<Column> columns = new LinkedHashSet<>(this.getColumns());
        columns.add(c);
        return new ResultSetSchema(columns, this.getGranularity());
    }

    @Override
    public Granularity getGranularity() {
        return granularity;
    }
}
