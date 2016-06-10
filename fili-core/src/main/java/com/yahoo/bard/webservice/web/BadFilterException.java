// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Bad filter exception is an exception encountered when the filter object cannot be built.
 */
public class BadFilterException extends Exception {

    //constructor with message.
    public BadFilterException(String message) {
        super(message);
    }

    public BadFilterException(String message, Throwable cause) {
        super(message, cause);
    }
}
