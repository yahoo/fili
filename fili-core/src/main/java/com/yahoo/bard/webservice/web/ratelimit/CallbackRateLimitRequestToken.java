// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit;

import com.yahoo.bard.webservice.web.RateLimitCleanupOnRequestComplete;
import com.yahoo.bard.webservice.web.RateLimitRequestToken;

/**
 * Request token that takes a rateLimitCleanup object on creation, and calls that to handle the cleanup when the request
 * transaction is completed.
 */
public class CallbackRateLimitRequestToken implements RateLimitRequestToken {
    private RateLimitCleanupOnRequestComplete rateLimitCleanup;
    private boolean isBound;

    /**
     * Constructor.
     *
     * @param isBound  Indicate whether or not the request associated with this token is valid (bound) or not
     * @param rateLimitCleanup  The callback for cleaning up after the request associated with this token is complete
     */
    CallbackRateLimitRequestToken(boolean isBound, RateLimitCleanupOnRequestComplete rateLimitCleanup) {
        this.rateLimitCleanup = rateLimitCleanup;
        this.isBound = isBound;
    }

    @Override
    public boolean isBound() {
        return isBound;
    }

    @Override
    public boolean bind() {
        return isBound;
    }

    @Override
    public void unBind() {
        close();
    }

    @Override
    public void close() {
        if (this.isBound) {
            rateLimitCleanup.cleanup();
            this.isBound = false;
        }
    }

    //TODO Take username for logging? Is it ok to just not log this, or log without passing username?
    @Override
    protected void finalize() throws Throwable {
        try {
            if (isBound) {
                close();
            }
        } finally {
            super.finalize();
        }
    }
}
