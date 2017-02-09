// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.joda.time.DateTimeZone;

import javax.validation.constraints.NotNull;

/**
 * A schema anchored to a particular time zone.
 *
 * @deprecated This class is no longer used to support ResultSet schemas
 */
@Deprecated
public class ZonedSchema extends BaseSchema implements Schema {

    private final DateTimeZone dateTimeZone;
    private final Granularity granularity;

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
        super(columns);
        this.granularity = granularity;
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

    /**
     * Granularity.
     *
     * @return the granularity for this schema
     */
    public Granularity getGranularity() {
        return granularity;
    }
}
