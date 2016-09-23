// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Bad having exception is an exception encountered when the having object cannot be built.
 */
public class BadHavingException extends Exception {

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     */
    public BadHavingException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message  Message of the exception
     * @param cause  Cause of the exception
     */
    public BadHavingException(String message, Throwable cause) {
        super(message, cause);
    }
}
