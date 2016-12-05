// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator;

/**
 * The sentinel has the lowest possible precedence, and doesn't
 * represent an actual operator.
 */
public class Sentinel implements Operator {

    @Override
    public Precedence getPrecedence() {
        return Precedence.SENTINEL;
    }

    @Override
    public int getNumOperands() {
        return 0;
    }
}
