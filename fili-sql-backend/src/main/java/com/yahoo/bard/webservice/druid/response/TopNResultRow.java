// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * Row of events in a TopNQuery.
 * todo: use and test
 */
@JsonPropertyOrder({"timestamp", "event"})
public class TopNResultRow extends DruidResultRow {

    @JsonProperty
    @JsonFormat(shape = JsonFormat.Shape.ARRAY)
    private final Map<String, Object> event = new HashMap<>();

    /**
     * Creates a row with the given timestamp.
     *
     * @param timestamp  The timestamp to set the event for.
     */
    public TopNResultRow(DateTime timestamp) {
        super(timestamp);
    }

    @Override
    public void add(String key, Object value) {
        event.put(key, value);
    }
}
