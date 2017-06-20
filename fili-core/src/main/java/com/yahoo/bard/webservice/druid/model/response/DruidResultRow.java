// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.Map;

/**
 * A row of results in a Druid Response.
 */
public abstract class DruidResultRow {
    @JsonIgnore
    protected final Map<String, Object> results;
    @JsonIgnore
    private final DateTime timestamp;

    /**
     * Creates a DruidResultRow at the given timestamp.
     *
     * @param timestamp  The timestamp to be included in serialization of a response.
     */
    DruidResultRow(DateTime timestamp) {
        this.timestamp = timestamp;
        results = new HashMap<>();
    }

    @JsonProperty
    public String getTimestamp() {
        return timestamp.toDateTime(DateTimeZone.UTC).toString();
    }

    /**
     * Adds a json key/value pair to the row.
     *
     * @param key  The key to be added.
     * @param value  The value of the key.
     */
    public void add(String key, Object value) {
        results.put(key, value);
    }

    /**
     * Gets the stored results for this result row.
     *
     * @return the stored results.
     */
    public abstract Map<String, Object> getResults();
}
