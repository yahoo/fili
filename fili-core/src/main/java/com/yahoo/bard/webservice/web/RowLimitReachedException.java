// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * A request-related or resources-related limit has been reached.
 */
public class RowLimitReachedException extends RuntimeException {

    /**
     * Constructor.
     */
    public RowLimitReachedException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param message  Exception message
     */
    public RowLimitReachedException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message  Exception message
     * @param cause  Cause of the exception
     */
    public RowLimitReachedException(String message, Throwable cause) {
        super(message, cause);
    }
}
