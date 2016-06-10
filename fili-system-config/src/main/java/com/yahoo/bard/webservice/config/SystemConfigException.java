// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

/**
 * Exception for failures from the system configuration code
 */
public class SystemConfigException extends Error {

    /**
     * Constructor that accepts a message
     * @param message  exception message
     */
    public SystemConfigException(String message) {
        super(message);
    }

    public SystemConfigException(String message, Throwable e) {
        super(message, e);
    }

    public SystemConfigException(Throwable e) {
        super(e);
    }
}
