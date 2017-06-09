// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * A row of results in a Druid Response.
 */
public abstract class DruidResultRow {
    @JsonIgnore
    public final DateTime timestamp;

    /**
     * Creates a DruidResultRow at the given timestamp.
     *
     * @param timestamp  The timestamp to be included in serialization of a response.
     */
    public DruidResultRow(DateTime timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty
    public String getTimestamp() {
        return timestamp.toDateTime(DateTimeZone.UTC).toString();
    }
}
