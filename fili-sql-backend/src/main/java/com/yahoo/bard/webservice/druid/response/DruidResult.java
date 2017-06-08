// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Created by hinterlong on 6/1/17.
 */
public class DruidResult {
    @JsonIgnore
    public final DateTime timestamp;

    public DruidResult(DateTime timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty
    public String getTimestamp() {
        return timestamp.toDateTime(DateTimeZone.UTC).toString();
    }
}
