// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Set;

/**
 * Records the names of all the threads that handled part of this request.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Threads implements LogInfo {
    protected final Set<String> threads;

    /**
     * Constructor.
     *
     * @param threadNames  The threads that a request got processed on
     */
    public Threads(Set<String> threadNames) {
        threads = threadNames;
    }

    @JsonValue
    public Set<String> getThreads() {
        return threads;
    }
}
