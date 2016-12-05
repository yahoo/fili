// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator

import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException
import com.yahoo.bard.webservice.data.config.metric.parser.operand.AndFilterNode
import com.yahoo.bard.webservice.data.config.metric.parser.operand.Operand
import com.yahoo.bard.webservice.data.config.metric.parser.operand.OrFilterNode
import com.yahoo.bard.webservice.data.config.metric.parser.operand.SelectorFilterNode

import spock.lang.Specification

public class BinaryFilterOperatorSpec extends Specification {

    def "Equals should produce a SelectorFilter"() {
        setup:
        def operator = BinaryFilterOperator.fromString("==")
        def filterNode = operator.build([Mock(Operand), Mock(Operand)])

        expect:
        operator.greaterThan(new Sentinel())
        filterNode instanceof SelectorFilterNode
    }

    def "And should produce an AndFilter"() {
        setup:
        def operator = BinaryFilterOperator.fromString("&&")
        def filterNode = operator.build([Mock(Operand), Mock(Operand)])

        expect:
        operator.greaterThan(new Sentinel())
        filterNode instanceof AndFilterNode
    }

    def "Or should produce an OrFilter"() {
        setup:
        def operator = BinaryFilterOperator.fromString("||")
        def filterNode = operator.build([Mock(Operand), Mock(Operand)])

        expect:
        operator.greaterThan(new Sentinel())
        filterNode instanceof OrFilterNode
    }

    def "invalid should throw an exception"() {
        when:
        BinaryFilterOperator.fromString("?")

        then:
        ParsingException ex = thrown()
        ex.message =~ /.*unsupported.*/
    }
}
