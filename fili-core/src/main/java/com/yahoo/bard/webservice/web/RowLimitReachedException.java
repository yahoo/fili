// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * A request-related or resources-related limit has been reached
 */
public class RowLimitReachedException extends RuntimeException {

    public RowLimitReachedException() {
        super();
    }

    public RowLimitReachedException(String string) {
        super(string);
    }

    public RowLimitReachedException(String message, Throwable cause) {
        super(message, cause);
    }
}
