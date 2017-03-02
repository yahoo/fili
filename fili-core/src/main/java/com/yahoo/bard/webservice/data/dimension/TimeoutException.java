// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

/**
 * Exception used by search providers to timeout queries.
 */
public class TimeoutException extends RuntimeException {

    /**
     * Constructor.
     * @param message Describes the reason for the timeout.
     */
    public TimeoutException(String message) {
        super(message);
    }


    /**
     * Constructor.
     * @param message Describes the reason for the timeout.
     * @param cause Wraps the generating exception.
     */
    public TimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
