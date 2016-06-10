// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

/**
 * Column Type found in schema that is not understood by parser
 */
public class UnknownColumnTypeException extends RuntimeException {

    public UnknownColumnTypeException() {
        super();
    }

    public UnknownColumnTypeException(String string) {
        super(string);
    }

    public UnknownColumnTypeException(String message, Throwable cause) {
        super(message, cause);
    }
}
