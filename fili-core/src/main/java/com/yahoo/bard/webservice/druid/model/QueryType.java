// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model;

import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Druid Queries the application knows about.
 */
public enum QueryType {
    GROUP_BY,
    TOP_N,
    TIMESERIES,
    TIME_BOUNDARY,
    SEGMENT_METADATA,
    SEARCH,
    LOOKBACK;

    final String jsonName;

    QueryType() {
        this.jsonName = EnumUtils.enumJsonName(this);
    }

    @JsonValue
    public String toJson() {
        return jsonName;
    }
}
