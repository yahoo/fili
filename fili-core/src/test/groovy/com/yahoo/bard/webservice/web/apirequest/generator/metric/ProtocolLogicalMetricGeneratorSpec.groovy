// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric

import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetricAnnotater

import spock.lang.Specification

class ProtocolLogicalMetricGeneratorSpec extends Specification {

    def "Unresolvable metrics fail the query" () {
        setup:
        ProtocolLogicalMetricGenerator generator = new ProtocolLogicalMetricGenerator(ApiMetricAnnotater.NO_OP_ANNOTATER, [])
        MetricDictionary metricDictionary = new MetricDictionary()
        List<ApiMetric> missingMetrics = [
                new ApiMetric(
                        "missingMetric",
                        "missingMetric",
                        [:]
                )
        ]

        when:
        generator.applyProtocols(missingMetrics, metricDictionary)

        then:
        thrown(BadApiRequestException)
    }

    def ""

    // Tests to write:

    // validation tests:
    // * validate checks base names of protocol metrics
    // * checking base names does NOT mess up validating standard metrics

    // binding tests:
    // * globbed name is used as output name.
    // * resulting protocol metrics always track base name.
}
