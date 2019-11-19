// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

/**
 * Exception when a dimension row is not found.
 */
public class DimensionRowNotFoundException extends FilterBuilderException {

    /**
     * Constructor that accepts a message.
     *
     * @param message  error message
     */
    public DimensionRowNotFoundException(String message) {
        super(message);
    }
}
