// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker
import com.yahoo.bard.webservice.data.config.metric.parser.operand.FunctionMetricNode
import com.yahoo.bard.webservice.data.config.metric.parser.operand.Operand

import spock.lang.Specification

public class FunctionOperatorSpec extends Specification {
    def "FunctionOperator should return a FunctionMetricNode and know its operand count"() {
        setup:
        def mockOperands = [Mock(Operand)]
        def mockMaker = Mock(MetricMaker)
        def operator = new FunctionOperator(mockMaker, 7)
        FunctionMetricNode result = operator.build(mockOperands)

        expect:
        operator.getNumOperands() == 7
        operator.greaterThan(new Sentinel())
        result.operands == mockOperands
        result.maker == mockMaker
    }
}
