// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator;

import com.yahoo.bard.webservice.data.config.metric.parser.operand.Operand;

import java.util.List;

/**
 * Interface representing operators in the metric definition language.
 */
public interface Operator {

    /**
     * Given a list of operands, build a MetricNode representing the computation operator(operands).
     *
     * @param operands Operator arguments
     * @return A MetricNode containing the expression
     */
    default Operand build(List<Operand> operands) {
        throw new UnsupportedOperationException("Operation not yet implemented.");
    }

    /**
     * Get the precedence of the operator.
     *
     * @return the precedence of the operator
     */
    Precedence getPrecedence();

    /**
     * Get the number of operands that the operator requires.
     *
     * @return the number of operands
     */
    int getNumOperands();

    /**
     * Determine whether this object or other has greater precedence.
     *
     * @param other operator to compare to
     * @return true if this object's precedence is higher; false otherwise
     */
    default boolean greaterThan(Operator other) {
        return getPrecedence().compareTo(other.getPrecedence()) > 0;
    }
}
