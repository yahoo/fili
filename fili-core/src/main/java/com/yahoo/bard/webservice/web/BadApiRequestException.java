// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Unchecked exception for a bad API request.
 */
public class BadApiRequestException extends RuntimeException {

    // Constructor that accepts a message
    public BadApiRequestException(String message) {
        super(message);
    }

    public BadApiRequestException(Throwable cause) {
        super(cause);
    }

    public BadApiRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
