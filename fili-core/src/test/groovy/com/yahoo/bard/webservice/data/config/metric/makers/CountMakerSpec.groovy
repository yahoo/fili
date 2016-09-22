// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.data.config.metric.MetricInstance
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.druid.model.aggregation.CountAggregation

import spock.lang.Specification

/**
 * Test count maker creates a metric correctly
 */
class CountMakerSpec extends Specification {

    MetricDictionary metricDictionary = new MetricDictionary()
    CountMaker countMaker = new CountMaker(metricDictionary)

    def "Test building a cardinality metric meets expected values"() {
        setup:
        CountMaker countMaker = new CountMaker(metricDictionary)
        LogicalMetric actualMetric = countMaker.make("metricName", [])
        CountAggregation actual = (CountAggregation) actualMetric.getTemplateDruidQuery().getMetricField("metricName")

        expect:
        actual.getName() == "metricName"
        actual.fieldName == ""
    }

    def "Test building from MetricInstance"() {
        setup:
        MetricInstance metricInstance = new MetricInstance("metricName", countMaker)
        LogicalMetric actualMetric = metricInstance.make()
        CountAggregation actual = (CountAggregation) actualMetric.getTemplateDruidQuery().getMetricField("metricName")

        expect:
        actual.getName() == "metricName"
        actual.fieldName == ""
    }
}
