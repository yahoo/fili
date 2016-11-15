// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.lexer;

import java.io.IOException;

/**
 * A LexException may be thrown if errors are found when lexing the metric string.
 */
public class LexException extends IOException {

    /**
     * Exception that is thrown when lexing fails.
     *
     * @param message the error message
     * @param current the current position while lexing
     */
    public LexException(String message, String current) {
        super(message + "; (parsing failed beginning at: " + current + ")");
    }
}
