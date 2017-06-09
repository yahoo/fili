// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.response;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.List;

/**
 * A response which serializes to an equivalent DruidResponse.
 *
 * @param <Row>  The type of {@link DruidResultRow} to make responses for.
 */
@JsonSerialize(using = DruidResponseSerializer.class)
public class DruidResponse<Row extends DruidResultRow> {
    /*
      todo figure out druid response layout
      TimeBoundary  same as  TimeseriesResultRow?
     */

    private final List<Row> results = new ArrayList<>();

    /**
     * Adds a row to the list of results.
     *
     * @param row  The row to be added.
     */
    public void add(Row row) {
        results.add(row);
    }

    /**
     * Gets the list of results for this response.
     *
     * @return the results.
     */
    public List<Row> getResults() {
        return results;
    }
}
