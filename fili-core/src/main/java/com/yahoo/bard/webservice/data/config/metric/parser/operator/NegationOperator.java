// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator;

/**
 * Negation operator (-).
 *
 * Note: Not yet fully implemented
 */
public class NegationOperator implements Operator {

    @Override
    public Precedence getPrecedence() {
        return Precedence.NEGATION;
    }

    public int getNumOperands() {
        return 1;
    }
}
