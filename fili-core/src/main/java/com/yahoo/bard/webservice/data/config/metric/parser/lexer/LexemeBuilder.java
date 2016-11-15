// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.lexer;

/**
 * Given an input string, produce the Lexeme at the beginning of the string.
 */
public interface LexemeBuilder {

    /**
     * Build a lexeme.
     *
     * May only consume part of the input string; you must check
     * Lexeme.getConsumedLength() to see how much of the string was consumed.
     *
     * @param inputString the string to consume a lexeme from
     * @return a Lexeme
     */
    Lexeme build(String inputString);
}
