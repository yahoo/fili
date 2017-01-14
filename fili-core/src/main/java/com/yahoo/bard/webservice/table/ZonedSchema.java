// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.joda.time.DateTimeZone;

import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * A schema anchored to a particular time zone.
 *
 * @deprecated This class is no longer used to support ResultSet schemas
 */
@Deprecated
public class ZonedSchema extends BaseSchema implements GranularSchema {

    private final DateTimeZone dateTimeZone;
    private final Granularity granularity;

    /**
     * Constructor.
     *
     * @param granularity  Granularity of the schema
     * @param dateTimeZone  TimeZone of the schema
     */
    public ZonedSchema(@NotNull Set<Column> columns, @NotNull Granularity granularity, @NotNull DateTimeZone dateTimeZone) {
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
        this(schema.getColumns(), schema.getGranularity(), schema.getDateTimeZone());
    }
    public DateTimeZone getDateTimeZone() {
        return dateTimeZone;
    }

    @Override
    public Granularity getGranularity() {
        return granularity;
    }
}
