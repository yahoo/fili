// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Row of events in a TopNQuery.
 */
@JsonPropertyOrder({"timestamp", "result"})
public class TopNResultRow extends DruidResultRow {

    private final List<Map<String, Object>> topNResults;

    /**
     * Creates a row with the given timestamp.
     *
     * @param timestamp  The timestamp to set the event for.
     */
    TopNResultRow(DateTime timestamp) {
        super(timestamp);
        topNResults = new ArrayList<>();
        topNResults.add(results);
    }

    @Override
    @JsonIgnore
    public Map<String, Object> getResults() {
        return results;
    }

    /**
     * Combines another TopNResultRow's results into this time bucket.
     *
     * @param topNResultRow  The row to combine with this one.
     */
    protected void addNextResult(TopNResultRow topNResultRow) {
        topNResults.add(topNResultRow.getResults());
    }

    @JsonProperty(value = "result")
    public List<Map<String, Object>> getTopNResults() {
        return topNResults;
    }
}
