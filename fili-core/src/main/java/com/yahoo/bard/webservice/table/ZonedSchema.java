// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.joda.time.DateTimeZone;

import javax.validation.constraints.NotNull;

/**
 * A schema anchored to a particular time zone
 */
public class ZonedSchema extends Schema {

    private final DateTimeZone dateTimeZone;

    public ZonedSchema(@NotNull Granularity granularity, @NotNull DateTimeZone dateTimeZone) {
        super(granularity);
        this.dateTimeZone = dateTimeZone;
    }

    public DateTimeZone getDateTimeZone() {
        return dateTimeZone;
    }
}
