// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand

import com.yahoo.bard.webservice.data.config.provider.FuzzyQueryMatcher
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation

import spock.lang.Specification
import spock.lang.Unroll

public class ArithmeticMetricNodeSpec extends Specification {

    @Unroll
    def "All types of arithmetic should be supported"(ArithmeticPostAggregation.ArithmeticPostAggregationFunction func) {
        setup:
        def arithNode = new ArithmeticMetricNode(func, new ConstantMetricNode("3"), new ConstantMetricNode("5"))
        def metric = arithNode.make("result", new MetricDictionary())

        expect:
        // Checks the names of top level fields, but not inner field names
        FuzzyQueryMatcher.matches(
                metric.getTemplateDruidQuery(),
                new TemplateDruidQuery(
                        [],
                        [new ArithmeticPostAggregation("result", func, [new ConstantPostAggregation("three", 3), new ConstantPostAggregation("five", 5)]) ]
                )
        )

        where:
        func << ArithmeticPostAggregation.ArithmeticPostAggregationFunction.values()
    }

    def "ArithmeticNode should make() inner metrics and add self to dictionary"() {
        setup:
        def dict = new MetricDictionary()

        def leftNode = Mock(MetricNode)
        def rightNode = Mock(MetricNode)

        def leftMetric = Mock(LogicalMetric)
        leftMetric.getTemplateDruidQuery() >> new TemplateDruidQuery([new LongSumAggregation("left", "left")], [])
        leftMetric.getName() >> "left"

        def rightMetric = Mock(LogicalMetric)
        rightMetric.getTemplateDruidQuery() >> new TemplateDruidQuery([new LongSumAggregation("right", "right")], [])
        rightMetric.getName() >> "right"

        dict.put("left", leftMetric)
        dict.put("right", rightMetric)

        def arithNode = new ArithmeticMetricNode(ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS, leftNode, rightNode)

        when:
        def metric = arithNode.make("result", dict)

        then:
        // The inner nodes should be made
        1 * leftNode.make(_, dict) >> leftMetric
        1 * rightNode.make(_, dict) >> rightMetric
        // The
        metric.getName() == "result"
        dict.containsKey("result")
    }
}

