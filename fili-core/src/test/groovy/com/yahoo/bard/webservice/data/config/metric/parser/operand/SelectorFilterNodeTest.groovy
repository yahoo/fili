// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter

import spock.lang.Specification

public class SelectorFilterNodeTest extends Specification {

    def "SelectorFilterNode should correctly build an SelectorFilter"() {
        setup:
        def dim = Mock(Dimension)
        def dimNode = Mock(DimensionNode)
        def constNode = Mock(ConstantMetricNode)

        // Expected interactions
        1 * dimNode.getDimension() >> dim
        1 * constNode.getValue() >> "1.0"

        def filter = new SelectorFilterNode(dimNode, constNode).buildFilter()

        expect:
        filter == new SelectorFilter(dim, "1.0")
    }
}
