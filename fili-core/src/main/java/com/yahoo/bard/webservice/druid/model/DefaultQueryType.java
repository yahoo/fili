// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model;

import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Druid queries that Fili supports out of the box.
 */
public enum DefaultQueryType implements QueryType {
    GROUP_BY,
    TOP_N,
    TIMESERIES,
    TIME_BOUNDARY,
    SEGMENT_METADATA,
    SEARCH,
    LOOKBACK;

    final String jsonName;

    /**
     * Constructor.
     */
    DefaultQueryType() {
        this.jsonName = EnumUtils.enumJsonName(this);
    }

    @Override
    public String toJson() {
        return jsonName;
    }
}
