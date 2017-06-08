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
 * Created by hinterlong on 6/1/17.
 */
@JsonPropertyOrder({"timestamp", "result"})
public class TopNResult extends DruidResult {

    @JsonProperty
    @JsonFormat(shape = JsonFormat.Shape.ARRAY)
    private final List<Object> result = new ArrayList<>();

    public TopNResult(DateTime timestamp) {
        super(timestamp);
    }

    public void add(Object value) {
        result.add(value);
    }
}
