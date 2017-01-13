// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.rfc.table;

import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ZonedSchema;

import org.joda.time.DateTimeZone;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.validation.constraints.NotNull;

public class ResultSetSchema extends ZonedSchema {

    public ResultSetSchema(
            Set<Column> columns,
            @NotNull Granularity granularity,
            DateTimeZone timeZone
    ) {
        super(columns, granularity, timeZone);
    }

    public ResultSetSchema(ZonedSchema zonedSchema) {
        super(zonedSchema);
    }

    public ResultSetSchema withAddColumn(Column c) {
        Set<Column> columns = new LinkedHashSet<>(this);
        return new ResultSetSchema(columns, this.getGranularity(), this.getDateTimeZone());
    }
}
