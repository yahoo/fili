// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.joda.time.DateTime;

import java.util.Map;

/**
 * Row of results in a {@link com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery}.
 */
@JsonPropertyOrder({"timestamp", "result"})
public class TimeseriesResultRow extends DruidResultRow {

    /**
     * Creates a row with the given timestamp.
     *
     * @param timestamp  The timestamp to set the result for.
     */
    TimeseriesResultRow(DateTime timestamp) {
        super(timestamp);
    }

    @Override
    @JsonProperty(value = "result")
    public Map<String, Object> getResults() {
        return results;
    }
}
