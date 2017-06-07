// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.mock;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hinterlong on 6/1/17.
 */
@JsonPropertyOrder({"timestamp", "result"})
public class TimeseriesResult extends DruidResult {
    @JsonProperty
    private final Map<String, Object> result = new HashMap<>();

    public TimeseriesResult(DateTime timestamp) {
        super(timestamp);
    }

    public void add(String key, Object value) {
        result.put(key, value);
    }

}
