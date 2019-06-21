// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

/**
 * Exception for failures from the system configuration code.
 */
public class SystemConfigException extends RuntimeException {

    /**
     * Constructor that accepts a message.
     *
     * @param message  exception message
     */
    public SystemConfigException(String message) {
        super(message);
    }

    /**
     * Constructor that accepts a Throwable.
     *
     * @param cause  The cause of this exception
     */
    public SystemConfigException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor that accepts a Throwable and a message.
     *
     * @param message  The message string for this exception
     * @param cause  The cause of this exception
     */
    public SystemConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
