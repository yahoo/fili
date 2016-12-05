// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand

import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker
import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException
import com.yahoo.bard.webservice.data.config.provider.FuzzyQueryMatcher
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation

import spock.lang.Specification

public class FunctionMetricNodeSpec extends Specification {
    def "FunctionMetricNode should properly construct aggregators"() {
        setup:
        def dict = new MetricDictionary()
        def f = new FunctionMetricNode(new LongSumMaker(dict), [new IdentifierNode("metric", null, dict)])
        def result = f.make("new_metric", dict)

        expect:
        dict.containsKey("new_metric")
        FuzzyQueryMatcher.matches(
                result.getTemplateDruidQuery(),
                new TemplateDruidQuery([new LongSumAggregation("new_metric", "metric")], [])
        )
    }

    def "Error should be thrown on bad operand"() {
        setup:
        def dict = new MetricDictionary()
        def f = new FunctionMetricNode(new LongSumMaker(dict), [Mock(FilterNode)])

        when:
        f.make("new_metric", dict)

        then:
        ParsingException ex = thrown()
        ex.message =~ /Unexpected node type.*/
    }

    def "Interactions with operands and maker should be correct"() {
        setup:

        // mock all the things
        def maker = Mock(MetricMaker)

        def makerMetric = Mock(LogicalMetric, { getName() >> "new_metric" })

        def op1 = Mock(MetricNode, { getMetricNode() >> it })
        def met1 = Mock(LogicalMetric, { getName() >> "metric1" })

        def op2 = Mock(MetricNode, { getMetricNode() >> it })
        def met2 = Mock(LogicalMetric, { getName() >> "metric2" })

        def dict = new MetricDictionary()
        def f = new FunctionMetricNode(maker, [op1, op2])

        when:
        def metric = f.make("new_metric", dict)

        then:
        1 * op1.make(_, _) >> met1
        1 * op2.make(_, _) >> met2
        1 * maker.make("new_metric", ["metric1", "metric2"]) >> makerMetric

        metric == makerMetric
    }
}
