// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * Row of results in a {@link com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery}.
 */
@JsonPropertyOrder({"timestamp", "result"})
public class TimeseriesResultRow extends DruidResultRow {
    @JsonProperty
    private final Map<String, Object> result = new HashMap<>();

    /**
     * Creates a row with the given timestamp.
     *
     * @param timestamp  The timestamp to set the result for.
     */
    public TimeseriesResultRow(DateTime timestamp) {
        super(timestamp);
    }

    /**
     * Adds a json key/value pair to the row.
     *
     * @param key  The key to be added.
     * @param value  The value of the key.
     */
    public void add(String key, Object value) {
        result.put(key, value);
    }
}
