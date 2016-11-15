// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser;

import java.io.IOException;

/**
 * Thrown when an error occurs while parsing.
 */
public class ParsingException extends IOException {

    /**
     * Construct a ParsingException with the given message.
     *
     * @param s message
     */
    public ParsingException(String s) {
        super(s);
    }

    /**
     * Construct a ParsingException with the given message and cause.
     *
     * @param s message
     * @param e cause
     */
    public ParsingException(String s, Throwable e) {
        super(s, e);
    }
}
