// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data;

/**
 * Exception during serialization.
 */
public class SerializationException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param message  String error message
     */
    public SerializationException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause  Throwable error object
     */
    public SerializationException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message  String error message
     * @param cause  Throwable error object
     */
    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
