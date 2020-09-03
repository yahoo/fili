// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import static com.yahoo.bard.webservice.util.JsonSortStrategy.SORT_BOTH

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.util.GroovyTestUtils

import spock.lang.Specification
import spock.lang.Timeout

@Timeout(30)    // Fail test if hangs
class MetricsServletSpec extends Specification {

    JerseyTestBinder jerseyTestBinder

    def setup() {
        // Create the test web container to test the resources
        jerseyTestBinder = new JerseyTestBinder(MetricsServlet.class)
        String DEFAULT_CATEGORY = "General"

        // Set known logical metrics
        NoOpResultSetMapper mapper = new NoOpResultSetMapper()

        //Rather than use the default TestMetricLoader data, throw it out and load a simpler data set
        jerseyTestBinder.configurationLoader.dictionaries.metricDictionary.clearLocal()
        ["metricA", "metricB", "metricC"].each { String metricName ->
            jerseyTestBinder.configurationLoader.metricDictionary.put(
                    metricName,
                    new LogicalMetricImpl(
                            null,
                            mapper,
                            new LogicalMetricInfo(metricName, metricName, DEFAULT_CATEGORY, metricName, "string")
                    )
            )
        }
    }

    def cleanup() {
        // Release the test web container
        jerseyTestBinder.tearDown()
    }

    def "test metrics endpoint"() {
        setup:

        String expectedResponse = """{
                                        "rows":
                                        [
                                            {"category": "General", "name":"metricA", "longName": "metricA", "type": "metricA", "type": "string", "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/metrics/metricA"},
                                            {"category": "General", "name":"metricB", "longName": "metricB", "type": "metricB", "type": "string", "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/metrics/metricB"},
                                            {"category": "General", "name":"metricC", "longName": "metricC", "type": "metricC", "type": "string", "uri":"http://localhost:${jerseyTestBinder.getHarness().getPort()}/metrics/metricC"}
                                        ]
                                    }"""

        when: "We send a request"
        String result = jerseyTestBinder.makeRequest("/metrics", [:]).get(String.class)

        then: "The response what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, SORT_BOTH)
    }

    def "test metric endpoint"() {
        setup:
        String expectedResponse = """{
                                        "category" : "General",
                                        "name" : "metricA",
                                        "longName" : "metricA",
                                        "description" : "metricA",
                                        "type": "string",
                                        "tables" : []
                                    }"""

        when: "We send a request"
        String result = jerseyTestBinder.makeRequest("/metrics/metricA", [:]).get(String.class)

        then: "The response what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse)
    }
}
