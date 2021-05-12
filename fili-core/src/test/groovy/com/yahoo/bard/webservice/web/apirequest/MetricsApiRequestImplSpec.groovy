// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.endpoints.MetricsServlet

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class MetricsApiRequestImplSpec extends Specification {

    JerseyTestBinder jtb

    @Shared
    MetricDictionary fullDictionary

    @Shared
    MetricDictionary emptyDictionary = new MetricDictionary()

    @Shared
    LinkedHashSet<LogicalMetric> metrics = new LinkedHashSet<>()

    LogicalMetric metric1
    LogicalMetric metric2

    def setup() {
        jtb = new JerseyTestBinder(MetricsServlet.class)
        fullDictionary = jtb.configurationLoader.metricDictionary
        metric1 = new LogicalMetricImpl(null, null, "met1")
        metric2 = new LogicalMetricImpl(null, null, "met2")
        metrics.add(metric1)
        metrics.add(metric2)
    }

    def cleanup() {
        jtb.tearDown()
    }

    def "check api request construction for the top level endpoint (all tables)"() {
        when:
        MetricsApiRequestImpl apiRequest = new MetricsApiRequestImpl(
                null,  // metricName
                null,  // format
                "",  // perPage
                "",  // page
                fullDictionary
        )

        then:
        apiRequest.getMetrics() as Set == fullDictionary.values() as Set
    }

    def "check api request construction for a given table name"() {
        setup:
        String name = "height"

        when:
        MetricsApiRequestImpl apiRequest = new MetricsApiRequestImpl(
                name,
                null,  // format
                "",  // perPage
                "",  // page
                fullDictionary
        )

        then:
        apiRequest.getMetric() == fullDictionary.get(name)
    }

    @Unroll
    def "api request construction throws #exception.simpleName because #reason"() {
        when:
        new MetricsApiRequestImpl(
                name,
                null,  // format
                "",  // perPage
                "",  // page
                dictionary
        )

        then:
        Exception e = thrown(exception)
        e.getMessage().matches(reason)

        where:
        name     | dictionary      | exception              | reason
        "height" | emptyDictionary | BadApiRequestException | ".*Metric.*do not exist.*"
        "weight" | fullDictionary  | BadApiRequestException | ".*Metric.*do not exist.*"
    }

    def "re-write api request with desired metrics"() {
        when:
        MetricsApiRequestImpl apiRequest = new MetricsApiRequestImpl(
                null,
                null,
                null,
                metrics
        )

        then:
        apiRequest.getMetrics().size() == 2
        apiRequest.getMetrics().getAt(0) == metric1
        apiRequest.getMetrics().getAt(1) == metric2
    }
}
