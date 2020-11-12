// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.exceptions;

/**
 * Bad having exception is an exception encountered when the having object cannot be built.
 */
public class BadOrderByException extends BadApiRequestException {

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     */
    public BadOrderByException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     * @param cause  Cause of the exception
     */
    public BadOrderByException(String message, Throwable cause) {
        super(message, cause);
    }
}
