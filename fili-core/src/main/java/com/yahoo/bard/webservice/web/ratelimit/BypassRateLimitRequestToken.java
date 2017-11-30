// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit;

import com.yahoo.bard.webservice.web.RateLimitRequestToken;

/**
 * RateLimitRequestToken for bypass request. Bypass requests are never rate limited.
 */
public class BypassRateLimitRequestToken implements RateLimitRequestToken {
    /**
     * Constructor.
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
