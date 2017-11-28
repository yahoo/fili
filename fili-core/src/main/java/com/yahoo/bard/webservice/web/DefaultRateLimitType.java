// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Type of rate limit.
 */
public enum DefaultRateLimitType implements RateLimitType {
    /**
     * The global cap on all allowable open queries has been hit.
     */
    GLOBAL,

    /**
     * An individual user has hit the cap on the amount of in-flight queries they are allow to send.
     */
    USER
}
