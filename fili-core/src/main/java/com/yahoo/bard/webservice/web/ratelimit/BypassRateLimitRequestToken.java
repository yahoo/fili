// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit;

/**
 * RateLimitRequestToken for bypass request. Bypass requests are never rate limited.
 */
public class BypassRateLimitRequestToken implements RateLimitRequestToken {

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
}
