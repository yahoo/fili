// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand

import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException
import com.yahoo.bard.webservice.druid.model.filter.Filter

import spock.lang.Specification

public class FilterNodeSpec extends Specification {
    def "should construct and filter"() {
        setup:
        def left = Mock(FilterNode)
        def right = Mock(FilterNode)

        when:
        def actual = FilterNode.create(Filter.DefaultFilterType.AND, left, right)

        then:
        1 * left.getFilterNode() >> left
        1 * right.getFilterNode() >> right

        actual instanceof AndFilterNode
    }

    def "should construct or filter"() {
        setup:
        def left = Mock(FilterNode)
        def right = Mock(FilterNode)

        when:
        def actual = FilterNode.create(Filter.DefaultFilterType.OR, left, right)

        then:
        1 * left.getFilterNode() >> left
        1 * right.getFilterNode() >> right

        actual instanceof OrFilterNode
    }

    def "should construct selector filter"() {
        setup:
        def left = Mock(DimensionNode)
        def right = Mock(ConstantMetricNode)

        when:
        def actual = FilterNode.create(Filter.DefaultFilterType.SELECTOR, left, right)

        then:
        1 * left.getDimensionNode() >> left
        1 * right.getConstantNode() >> right

        actual instanceof SelectorFilterNode
    }

    def "should throw exception for fields that aren't implemented yet"() {
        setup:
        def left = Mock(FilterNode)
        def right = Mock(DimensionNode)

        when:
        FilterNode.create(Filter.DefaultFilterType.NOT, left, right)

        then:
        ParsingException ex = thrown()
        ex.message =~ /Could not handle filter type:.*/
    }
}
