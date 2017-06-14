// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.response;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.List;

/**
 * A response which serializes to an equivalent DruidResponse.
 */
@JsonSerialize(using = DruidResponseSerializer.class)
public class DruidResponse {
    private final List<DruidResultRow> results;

    /**
     * Construct a Druid Response which will store rows of results.
     */
    public DruidResponse() {
        results = new ArrayList<>();
    }

    /**
     * Adds a row to the list of results.
     *
     * @param row  The row to be added.
     */
    public void add(DruidResultRow row) {
        results.add(row);
    }

    /**
     * Gets the list of results for this response.
     *
     * @return the results.
     */
    public List<DruidResultRow> getResults() {
        return results;
    }
}
