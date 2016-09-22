// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

/**
 * Exception during deserialization.
 */
public class DeserializationException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param message  Message for the exception
     */
    public DeserializationException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause  Cause of the exception
     */
    public DeserializationException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message  Message for the exception
     * @param cause  Cause of the exception
     */
    public DeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
