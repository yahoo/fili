// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.response;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds DruidResultRows combining results with the same
 * timestamp which is needed for TopNQueries.
 */
@JsonSerialize(using = DruidResponseSerializer.class)
public class TopNDruidResponse extends DruidResponse {
    private final Map<String, DruidResultRow> timestampToResult;

    /**
     * Builds a TopNDruidResponse.
     */
    TopNDruidResponse() {
        timestampToResult = new HashMap<>();
    }

    @Override
    public void add(DruidResultRow row) {
        String timestamp = row.getTimestamp();
        if (timestampToResult.containsKey(timestamp)) {
            TopNResultRow existing = (TopNResultRow) timestampToResult.get(timestamp);
            existing.addNextResult((TopNResultRow) row);
        } else {
            super.add(row);
            timestampToResult.put(timestamp, row);
        }
    }
}
