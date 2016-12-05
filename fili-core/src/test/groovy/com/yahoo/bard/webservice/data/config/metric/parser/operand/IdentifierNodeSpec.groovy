// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary

import spock.lang.Specification

public class IdentifierNodeSpec extends Specification {
    def "should be able to get identifier as metric"() {
        setup:
        def dict = new MetricDictionary()
        def expected = Mock(LogicalMetric)
        dict.put("node", expected)
        def node = new IdentifierNode("node", Mock(DimensionDictionary), dict)

        when:
        def actual = node.getMetricNode().make("some metric", dict)

        then:
        actual == expected
    }

    def "should be able to get identifier as dimension"() {

        setup:
        def dict = new DimensionDictionary()
        def expected = Mock(Dimension)
        expected.getApiName() >> "node"
        dict.add(expected)
        def node = new IdentifierNode("node", dict, Mock(MetricDictionary))

        when:
        def actual = node.getDimensionNode().getDimension()

        then:
        actual == expected
    }

}
