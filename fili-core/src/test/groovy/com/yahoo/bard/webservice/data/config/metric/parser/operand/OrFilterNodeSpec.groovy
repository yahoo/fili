// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand

import com.yahoo.bard.webservice.druid.model.filter.OrFilter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter

import spock.lang.Specification

public class OrFilterNodeSpec extends Specification {

    def "OrFilterNode should correctly build an OrFilter"() {
        setup:
        def left = Mock(SelectorFilter)
        def right = Mock(SelectorFilter)
        def leftNode = Mock(FilterNode)
        def rightNode = Mock(FilterNode)

        // Expected interactions
        1 * leftNode.buildFilter() >> left
        1 * rightNode.buildFilter() >> right

        def filter = new OrFilterNode(leftNode, rightNode).buildFilter()

        expect:
        filter == new OrFilter([left, right])
    }
}
