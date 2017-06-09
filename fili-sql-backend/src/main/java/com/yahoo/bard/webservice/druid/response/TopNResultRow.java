// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * Row of results in a TopNQuery.
 * todo: use and test
 */
@JsonPropertyOrder({"timestamp", "result"})
public class TopNResultRow extends DruidResultRow {

    @JsonProperty
    @JsonFormat(shape = JsonFormat.Shape.ARRAY)
    private final List<Object> result = new ArrayList<>();

    /**
     * Creates a row with the given timestamp.
     *
     * @param timestamp  The timestamp to set the result for.
     */
    public TopNResultRow(DateTime timestamp) {
        super(timestamp);
    }

    /**
     * Adds an object to the list of results.
     *
     * @param value  The result to add.
     */
    public void add(Object value) {
        result.add(value);
    }
}
