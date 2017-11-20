// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.security.Principal;

/**
 * An object containing the logic to keep track of and limit the amount of requests being made to the webservice.
 */
public interface RateLimiter {

    /**
     * Request a token from the RateLimiter, which represents an in-flight request.
     *
     * @param type  The type of request.
     * @param user  The user making the request.
     * @return a RequestToken object representing a request.
     */
    RequestToken getToken(RequestType type, Principal user);
}
