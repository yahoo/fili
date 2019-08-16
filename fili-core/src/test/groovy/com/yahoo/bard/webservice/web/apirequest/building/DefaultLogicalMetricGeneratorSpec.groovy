// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.building

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.web.BadApiRequestException
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.apirequest.binders.LogicalMetricGenerator

import spock.lang.Specification

class DefaultLogicalMetricGeneratorSpec extends Specification {
    def "generateLogicalMetrics() returns existing LogicalMetrics"() {
        given: "two LogicalMetrics in MetricDictionary"
        LogicalMetric logicalMetric1 = Mock(LogicalMetric)
        LogicalMetric logicalMetric2 = Mock(LogicalMetric)
        MetricDictionary metricDictionary = Mock(MetricDictionary)
        metricDictionary.get("logicalMetric1") >> logicalMetric1
        metricDictionary.get("logicalMetric2") >> logicalMetric2

        expect: "the two metrics are returned on request"
        LogicalMetricGenerator.DEFAULT_LOGICAL_METRIC_GENERATOR.generateLogicalMetrics("logicalMetric1,logicalMetric2", metricDictionary) ==
                [logicalMetric1, logicalMetric2] as LinkedHashSet
    }

    def "generateLogicalMetrics() throws BadApiRequestException on non-existing LogicalMetric"() {
        given: "a MetricDictionary"
        LogicalMetric logicalMetric = Mock(LogicalMetric)
        MetricDictionary metricDictionary = Mock(MetricDictionary)
        metricDictionary.get("logicalMetric") >> logicalMetric

        when: "a non-existing metrics request"
        LogicalMetricGenerator.DEFAULT_LOGICAL_METRIC_GENERATOR.generateLogicalMetrics("nonExistingMetric", metricDictionary)

        then: "BadApiRequestException is thrown"
        BadApiRequestException exception = thrown()
        exception.message == ErrorMessageFormat.METRICS_UNDEFINED.logFormat(["nonExistingMetric"])
    }
}
