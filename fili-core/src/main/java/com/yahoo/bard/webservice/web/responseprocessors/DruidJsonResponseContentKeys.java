// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

/**
 * Enumerates the list of keys expected to be found in the FullResponseProcessor.
 */
public enum DruidJsonResponseContentKeys {
    DRUID_RESPONSE_CONTEXT("X-Druid-Response-Context"),
    UNCOVERED_INTERVALS("uncoveredIntervals"),
    UNCOVERED_INTERVALS_OVERFLOWED("uncoveredIntervalsOverflowed"),
    STATUS_CODE("status-code"),
    RESPONSE("response"),
    ETAG("Etag"),
    CACHED_RESPONSE("cachedResponse")
    ;

    private final String name;

    /**
     * Constructor.
     *
     * @param name  Name of the context key
     */
    DruidJsonResponseContentKeys(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
