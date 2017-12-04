// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit;

/**
 * Cleans up the resources related to a RateLimiter tracking an outstanding request.
 */
@FunctionalInterface
public interface RateLimitCleanupOnRequestComplete {

    /**
     * Perform the cleanup.
     */
    void cleanup();
}
