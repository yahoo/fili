// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request token that takes a rateLimitCleanup object on creation, and calls that to handle the cleanup when the request
 * transaction is completed.
 */
public class CallbackRateLimitRequestToken implements RateLimitRequestToken {
    private static final Logger LOG = LoggerFactory.getLogger(CallbackRateLimitRequestToken.class);

    private final RateLimitCleanupOnRequestComplete rateLimitCleanup;
    private boolean isBound;

    /**
     * Constructor.
     *
     * @param isBound  Indicate whether or not the request associated with this token is valid (bound) or not
     * @param rateLimitCleanup  The callback for cleaning up after the request associated with this token is complete
     */
    public CallbackRateLimitRequestToken(boolean isBound, RateLimitCleanupOnRequestComplete rateLimitCleanup) {
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
        if (this.isBound) {
            rateLimitCleanup.cleanup();
            this.isBound = false;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (isBound) {
                LOG.debug("orphaned CallbackRateLimitToken");
                unBind();
            }
        } finally {
            super.finalize();
        }
    }
}
