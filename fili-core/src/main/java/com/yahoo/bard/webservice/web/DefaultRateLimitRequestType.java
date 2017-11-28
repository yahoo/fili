// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Type of outstanding request.
 */
public enum DefaultRateLimitRequestType implements RateLimitRequestType {
    /**
     * User made request, generally generated from direct webservice queries.
     */
    USER,

    /**
     * UI made request, generally generated from web frontend.
     */
    UI,

    /**
     * Request to be always run and ignored by rate limiter.
     */
    BYPASS;

    private final String name;

    /**
     * Sets the name of enum value when it is created.
     */
    DefaultRateLimitRequestType() {
        this.name = name();
    }

    @Override
    public String getName() {
        return name;
    }
}
