// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator;

/**
 * The precedence of a given operation.
 */
public enum Precedence {

    SENTINEL,
    ADD_SUB,
    MUL_DIV,
    FILTER,
    AND_OR,
    EQUALITY,
    NEGATION,
    FUNCTION;

    /**
     * Return true if this enum is greater than the other.
     *
     * @param other precedence to compare to
     * @return true if this precedence is greater than the given precedence
     */
    public boolean greaterThan(Precedence other) {
        return this.compareTo(other) > 0;
    }
}
