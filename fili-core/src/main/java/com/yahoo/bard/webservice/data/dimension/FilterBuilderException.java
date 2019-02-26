// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

/**
 * Exception when unable to correctly build query model filters.
 */
public class FilterBuilderException extends Exception {

    /**
     * Constructor that accepts a message.
     *
     * @param message  error message
     */
    public FilterBuilderException(String message) {
        super(message);
    }

    /**
     * Constructor that accepts a throwable.
     *
     * @param cause  underlying exception
     */
    public FilterBuilderException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor that accepts a throwable.
     *
     * @param message exception message
     * @param cause  underlying exception
     */
    public FilterBuilderException(String message, Throwable cause) {
        super(message, cause);
    }
}
