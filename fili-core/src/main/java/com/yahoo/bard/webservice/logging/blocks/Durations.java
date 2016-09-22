// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

/**
 * Records the durations of each timed phase of the request processing.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Durations implements LogInfo {
    protected final Map<String, Float> durations;

    /**
     * Constructor.
     *
     * @param durations  Map of durations
     */
    public Durations(Map<String, Float> durations) {
        this.durations = durations;
    }

    @JsonValue
    public Map<String, Float> getDurations() {
        return durations;
    }
}
