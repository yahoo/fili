// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.config.names.TestDruidMetricName
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.SketchRoundUpMapper

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.ThetaSketchAggregation

import spock.lang.Specification

class ThetaSketchMakerSpec extends Specification {

    static final int SKETCH_SIZE = 16000

    def "An aggregation returns the correct type"() {
        given: "A name for this , and the name of the metric this metric relies on"
        String metricName = TestApiMetricName.A_OTHER_USERS.asName()
        String dependentMetricName = TestDruidMetricName.USERS.asName()

        and: "An aggregation"
         Aggregation aggregation = new ThetaSketchAggregation(metricName, dependentMetricName, SKETCH_SIZE)

        expect:
        aggregation.getType() == "thetaSketch"
    }

    def "A Logical Metric that performs a sketch count is built correctly"() {
        given: "A name for this , and the name of the metric this metric relies on"
        String metricName = TestApiMetricName.A_OTHER_USERS.asName()
        String dependentMetricName = TestDruidMetricName.USERS.asName()

        and: "The logical metric the maker is expected to build"
        Set aggregations = [new ThetaSketchAggregation(metricName, dependentMetricName, SKETCH_SIZE)] as Set
        LogicalMetric expectedMetric = new LogicalMetric(
                new TemplateDruidQuery(aggregations, [] as Set),
                new SketchRoundUpMapper(metricName),
                metricName
        )

        and:
        MetricMaker maker = new ThetaSketchMaker(new MetricDictionary(), SKETCH_SIZE)

        expect:
        maker.make(metricName, [dependentMetricName]) == expectedMetric
    }
}
