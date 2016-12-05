// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand

import com.yahoo.bard.webservice.druid.model.filter.AndFilter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter

import spock.lang.Specification

public class AndFilterNodeSpec extends Specification {

    def "AndFilterNode should correctly build an AndFilter"() {
        setup:
        def left = Mock(SelectorFilter)
        def right = Mock(SelectorFilter)
        def leftNode = Mock(FilterNode)
        def rightNode = Mock(FilterNode)

        // Expected interactions
        1 * leftNode.buildFilter() >> left
        1 * rightNode.buildFilter() >> right

        def filter = new AndFilterNode(leftNode, rightNode).buildFilter()

        expect:
        filter == new AndFilter([left, right])
    }
}

