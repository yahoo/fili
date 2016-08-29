// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.jobs;

/**
 * Exception for invalid YCA certificate.
 */
public class InvalidYcaCertException extends RuntimeException {
    /**
     * Constructor.
     *
     * @param message  String error message
     * @param cause  Throwable error object
     */
    public InvalidYcaCertException(String message, Exception cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     *
     * @param message  String error message
     */
    public InvalidYcaCertException(String message) {
        super(message);
    }
}
