// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator

import com.yahoo.bard.webservice.data.config.metric.parser.operand.Operand

import spock.lang.Specification
import spock.lang.Unroll

public class ArithmeticOperatorSpec extends Specification {

    @Unroll
    def "All operations should behave correctly"(String operatorString, Precedence precedence) {
        setup:
        ArithmeticOperator operator = ArithmeticOperator.fromString(operatorString)
        def left = Mock(Operand)
        def right = Mock(Operand)

        when:
        operator.build([left, right])

        then:
        1 * left.getMetricNode()
        1 * right.getMetricNode()
        operator.greaterThan(new Sentinel())
        operator.getPrecedence() == precedence
        operator.getNumOperands() == 2

        where:
        operatorString | precedence
        "+" | Precedence.ADD_SUB
        "-" | Precedence.ADD_SUB
        "*" | Precedence.MUL_DIV
        "/" | Precedence.MUL_DIV

    }
}
