// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

/**
 * An exception for PhysicalTableMatchers when they return no matches.
 */
public class NoMatchFoundException extends Exception {
    /**
     * Constructor.
     *
     * @param message  Message for the exception
     */
    public NoMatchFoundException(String message) {
        super(message);
    }
}
