// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator;

import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.ArithmeticMetricNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.MetricNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.Operand;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;

import java.util.List;

/**
 * Binary arithmetic operators (+, -, *, /) take two operands.
 */
public enum ArithmeticOperator implements Operator {
    PLUS(ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS, Precedence.ADD_SUB),
    MINUS(ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MINUS, Precedence.ADD_SUB),
    MULTIPLY(ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MULTIPLY, Precedence.MUL_DIV),
    DIVIDE(ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE, Precedence.MUL_DIV);

    protected ArithmeticPostAggregation.ArithmeticPostAggregationFunction func;
    protected Precedence precedence;

    /**
     * Create a new arithmetic operator with the given function and precedence.
     *
     * @param func an arithmetic function
     * @param precedence the function's precedence
     */
    ArithmeticOperator(ArithmeticPostAggregation.ArithmeticPostAggregationFunction func, Precedence precedence) {
        this.func = func;
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
    public MetricNode build(List<Operand> operands) {
        return new ArithmeticMetricNode(func, operands.get(0).getMetricNode(), operands.get(1).getMetricNode());
    }

    /**
     * Create a new arithmetic operator from the given string.
     *
     * @param op string representing an operation
     * @return an ArithmeticOperator if string is valid
     * @throws ParsingException if an invalid string was passed
     */
    public static ArithmeticOperator fromString(String op) throws ParsingException {
        switch (op) {
            case "+":
                return PLUS;
            case "-":
                return MINUS;
            case "*":
                return MULTIPLY;
            case "/":
                return DIVIDE;
            default:
                throw new ParsingException("Got unsupported operator: " + op);
        }
    }
}
