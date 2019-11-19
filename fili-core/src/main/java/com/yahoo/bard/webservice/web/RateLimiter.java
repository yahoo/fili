// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.web.ratelimit.RateLimitRequestToken;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * An object containing the logic to keep track of and limit the amount of requests being made to the webservice.
 */
@FunctionalInterface
public interface RateLimiter {

    /**
     * Request a token from the RateLimiter, which represents an in-flight request.
     *
     * @param request  The object representing the request
     *
     * @return a RateLimitRequestToken object representing a request.
     */
    RateLimitRequestToken getToken(ContainerRequestContext request);
}
