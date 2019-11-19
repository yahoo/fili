// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.time.Granularity;

import org.joda.time.DateTimeZone;

import javax.validation.constraints.NotNull;

/**
 * A schema anchored to a particular time zone.
 *
 * @deprecated This class is no longer used as a subclass for {@link com.yahoo.bard.webservice.data.ResultSetSchema}.
 * Use that class directly now.
 */
@Deprecated
public class ZonedSchema extends BaseSchema implements Schema {

    private final DateTimeZone dateTimeZone;

    /**
     * Constructor.
     *
     * @param granularity  Granularity of the schema
     * @param dateTimeZone  TimeZone of the schema
     * @param columns The columns for this schema
     */
    public ZonedSchema(
            @NotNull Granularity granularity,
            @NotNull DateTimeZone dateTimeZone,
            @NotNull Iterable<Column> columns
    ) {
        super(granularity, columns);
        this.dateTimeZone = dateTimeZone;
    }

    /**
     * Constructor.
     *
     * @param schema schema to copy construct
     */
    public ZonedSchema(ZonedSchema schema) {
        this(schema.getGranularity(), schema.getDateTimeZone(), schema.getColumns());
    }
    public DateTimeZone getDateTimeZone() {
        return dateTimeZone;
    }
}
