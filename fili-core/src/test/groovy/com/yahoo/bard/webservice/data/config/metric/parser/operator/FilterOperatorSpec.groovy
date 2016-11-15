// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator

import com.yahoo.bard.webservice.data.config.metric.parser.operand.FilteredAggMetricNode
import com.yahoo.bard.webservice.data.config.metric.parser.operand.IdentifierNode
import com.yahoo.bard.webservice.data.config.metric.parser.operand.SelectorFilterNode

import spock.lang.Specification

public class FilterOperatorSpec extends Specification {
    def "FilterOperator should behave correctly"() {
        setup:
        def operator = new FilterOperator();

        def metricNode = Mock(IdentifierNode, {
            getMetricNode() >> it
            getIdentifierNode() >> it
        })
        def filterNode = Mock(SelectorFilterNode, {
            getFilterNode() >> it
        })

        FilteredAggMetricNode builtMetricNode = operator.build([metricNode, filterNode])

        expect:
        operator.getNumOperands() == 2
        operator.greaterThan(new Sentinel())

        // .equals() isn't implemented on everything
        builtMetricNode.left == metricNode
        builtMetricNode.right == filterNode
    }
}
