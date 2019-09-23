package com.yahoo.bard.webservice.web.apirequest.generator

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.web.BadApiRequestException
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder
import com.yahoo.bard.webservice.web.util.BardConfigResources

import spock.lang.Specification

class DefaultLogicalMetricGeneratorSpec extends Specification {

    Generator<LinkedHashSet<LogicalMetric>> gen

    def setup() {
        gen = new DefaultLogicalMetricGenerator()
    }

    def "generateLogicalMetrics() returns existing LogicalMetrics"() {
        setup: "prepare generator params"
        BardConfigResources resources = Mock(BardConfigResources)

        LogicalMetric logicalMetric1 = Mock(LogicalMetric)
        LogicalMetric logicalMetric2 = Mock(LogicalMetric)

        MetricDictionary metricDictionary = Mock(MetricDictionary)
        metricDictionary.get("logicalMetric1") >> logicalMetric1
        metricDictionary.get("logicalMetric2") >> logicalMetric2
        resources.getMetricDictionary() >> metricDictionary

        DataApiRequestBuilder builder = new DataApiRequestBuilder(resources)
        TestRequestParameters params = new TestRequestParameters()

        // requested metrics
        params.logicalMetrics =  "logicalMetric1,logicalMetric2"

        expect: "the two metrics are returned on request"
        gen.bind(builder, params, resources) == [logicalMetric1, logicalMetric2] as LinkedHashSet
    }

    def "generateLogicalMetrics() throws BadApiRequestException on non-existing LogicalMetric"() {
        setup:
        BardConfigResources resources = Mock(BardConfigResources)

        LogicalMetric logicalMetric = Mock(LogicalMetric)
        MetricDictionary metricDictionary = Mock(MetricDictionary)
        metricDictionary.get("logicalMetric") >> logicalMetric
        resources.getMetricDictionary() >> metricDictionary

        DataApiRequestBuilder builder = new DataApiRequestBuilder(resources)
        TestRequestParameters params = new TestRequestParameters()

        // requested metrics
        params.logicalMetrics =  "nonExistingMetric"

        when: "a non-existing metrics request"
        gen.bind(builder, params, resources)

        then: "BadApiRequestException is thrown"
        BadApiRequestException exception = thrown()
        exception.message == ErrorMessageFormat.METRICS_UNDEFINED.logFormat(["nonExistingMetric"])
    }
}
