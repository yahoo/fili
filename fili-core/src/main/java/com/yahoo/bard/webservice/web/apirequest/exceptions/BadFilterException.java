// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.exceptions;

/**
 * Bad filter exception is an exception encountered when the filter object cannot be built.
 */
public class BadFilterException extends Exception {

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     */
    public BadFilterException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     * @param cause  Cause of the exception
     */
    public BadFilterException(String message, Throwable cause) {
        super(message, cause);
    }
}
