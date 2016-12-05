// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation

import spock.lang.Specification

public class ConstantMetricNodeSpec extends Specification {

    def "ConstantMetricNode should correctly add a constant to metric dictionary"() {
        setup:
        def node = new ConstantMetricNode("3.14")
        def dict = new MetricDictionary()
        def actual = node.make("pi", dict)

        def expected = new LogicalMetric(
                new TemplateDruidQuery([], [new ConstantPostAggregation("pi", 3.14)]),
                new NoOpResultSetMapper(),
                "pi"
        )

        expect:
        node.getValue() == "3.14"
        actual == expected
        dict.get("pi") == expected
        dict.containsKey("pi")
    }
}

