// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Interface for rate limit request types.
 */
public interface RateLimitRequestType {
    /**
     * A way to query for the type of rate limit request.
     *
     * @return String representing rate limit request type.
     */
    String getType();
}
