// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.codahale.metrics.Meter;

/**
 * RateLimitRequestToken for rejected request.
 */
public class BypassRateLimitRequestToken implements RateLimitRequestToken {

    /**
     * Constructor.
     * <p>
     * Also counts the bypass token as being issued.
     *
     * @param requestBypassMeter  Bypass meter for tracking amount of bypass requests issued.
     */
    public BypassRateLimitRequestToken(Meter requestBypassMeter) {
        requestBypassMeter.mark();
    }

    /**
     * Constructor that does not require a meter.
     */
    public BypassRateLimitRequestToken() { }

    @Override
    public boolean isBound() {
        return true;
    }

    @Override
    public boolean bind() {
        return true;
    }

    @Override
    public void unBind() {
        // Do nothing
    }

    @Override
    public void close() {
        // Do nothing
    }
}
