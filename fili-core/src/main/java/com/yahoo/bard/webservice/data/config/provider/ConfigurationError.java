// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider;

/**
 * Exception that occurs during configuration.
 */
public class ConfigurationError extends Error {

    /**
     * Construct a new configuration exception with the given message.
     *
     * @param message  exception message
     */
    public ConfigurationError(String message) {
        super(message);
    }

    /**
     * Construct a new configuration exception with the given message and cause.
     *
     * @param message  exception message
     * @param cause  exception cause
     */
    public ConfigurationError(String message, Throwable cause) {
        super(message, cause);
    }
}
