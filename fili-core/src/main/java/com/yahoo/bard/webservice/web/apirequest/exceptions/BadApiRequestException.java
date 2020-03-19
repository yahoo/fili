// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.exceptions;

/**
 * Unchecked exception for a bad API request.
 */
public class BadApiRequestException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     */
    public BadApiRequestException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause  Cause of the exception
     */
    public BadApiRequestException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     * @param cause  Cause of the exception
     */
    public BadApiRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
