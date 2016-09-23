// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation

import spock.lang.Specification

class ConstantMakerSpec extends Specification {

    final String AGGREGATION_NAME = "one"
    final double CONSTANT_VALUE = 1.0

    def "A constant is built correctly when its 'dependent metric' is the string representation of a number."(){
        given: "A Logical Metric representing a constant of value CONSTANT_VALUE"
        PostAggregation postAggregation = new ConstantPostAggregation(AGGREGATION_NAME, CONSTANT_VALUE)
        TemplateDruidQuery constantQuery = new TemplateDruidQuery(
            [] as Set,
            [postAggregation] as Set
        )
        LogicalMetric expectedMetric = new LogicalMetric(
            constantQuery,
            new NoOpResultSetMapper(),
            AGGREGATION_NAME
        )

        and:
        //ConstantMakers don't rely on the metric dictionary
        MetricMaker maker = new ConstantMaker(null)

        expect:
        maker.make(AGGREGATION_NAME, Double.toString(CONSTANT_VALUE)) == expectedMetric
    }

    def """A NumberFormatException is thrown if the dependent metric passed to ConstantMaker is
           not a number."""(){
        given:
        String invalidValue = "I'm not a number."

        and:
        MetricMaker maker = new ConstantMaker(null)

        when:
        maker.make(AGGREGATION_NAME, invalidValue)

        then:
        thrown(NumberFormatException)
    }
}
