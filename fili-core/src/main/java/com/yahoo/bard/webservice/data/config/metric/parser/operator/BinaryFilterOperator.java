// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator;

import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.FilterNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.Operand;
import com.yahoo.bard.webservice.druid.model.filter.Filter;

import java.util.List;

/**
 * Binary filter operators ( &amp;&amp;, ||, ==) take two operands.
 */
public enum BinaryFilterOperator implements Operator {
    AND(Filter.DefaultFilterType.AND, Precedence.AND_OR),
    OR(Filter.DefaultFilterType.OR, Precedence.AND_OR),
    EQUALS(Filter.DefaultFilterType.SELECTOR, Precedence.EQUALITY);

    protected Precedence precedence;
    protected Filter.DefaultFilterType filterType;

    /**
     * Create a new binary filter operator.
     *
     * @param filterType the type of filter
     * @param precedence the precedence of the operation
     */
    BinaryFilterOperator(Filter.DefaultFilterType filterType, Precedence precedence) {
        this.filterType = filterType;
        this.precedence = precedence;
    }

    @Override
    public Precedence getPrecedence() {
        return precedence;
    }

    @Override
    public int getNumOperands() {
        return 2;
    }

    @Override
    public Operand build(List<Operand> operands) {
        return FilterNode.create(this.filterType, operands.get(0), operands.get(1));
    }

    /**
     * Create a new filter from the given string.
     *
     * @param op a string representing a filter
     * @return a BinaryFilterOperator if the given string was valid
     * @throws ParsingException an invalid string was passed
     */
    public static BinaryFilterOperator fromString(String op) throws ParsingException {
        switch (op) {
            case "&&":
                return AND;
            case "||":
                return OR;
            case "==":
                return EQUALS;
            default:
                throw new ParsingException("Got unsupported operator: " + op);
        }
    }
}
