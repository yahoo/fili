// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import static com.yahoo.bard.webservice.util.JsonSortStrategy.SORT_BOTH

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.util.GroovyTestUtils

import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.client.Invocation

@Timeout(30)    // Fail test if hangs
class MetricsServletSpec extends Specification {

    JerseyTestBinder jtb

    def setup() {
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(MetricsServlet.class)

        // Set known logical metrics
        NoOpResultSetMapper mapper = new NoOpResultSetMapper()

        //Rather than use the default TestMetricLoader data, throw it out and load a simpler data set
        jtb.configurationLoader.dictionaries.metricDictionary.clearLocal()
        ["metricA", "metricB", "metricC"].each { String metricName ->
            jtb.configurationLoader.metricDictionary.put(metricName, new LogicalMetric(null, mapper, metricName))
        }
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    def "test metrics endpoint"() {
        setup:

        String expectedResponse = """{
                                        "rows":
                                        [
                                            {"category": "General", "name":"metricA", "longName": "metricA", "uri":"http://localhost:9998/metrics/metricA"},
                                            {"category": "General", "name":"metricB", "longName": "metricB", "uri":"http://localhost:9998/metrics/metricB"},
                                            {"category": "General", "name":"metricC", "longName": "metricC", "uri":"http://localhost:9998/metrics/metricC"}
                                        ]
                                    }"""

        when: "We send a request"
        String result = makeRequest("/metrics", [:]).get(String.class)

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
                                        "tables" : []
                                    }"""

        when: "We send a request"
        String result = makeRequest("/metrics/metricA", [:]).get(String.class)

        then: "The response what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    Invocation.Builder makeRequest(String target, LinkedHashMap<String, Object> queryParams) {
        // Set target of call
        def httpCall = jtb.getHarness().target(target)

        // Add query params to call
        queryParams.each { String key, Object value ->
            httpCall = httpCall.queryParam(key, value)
        }

        // Make the call
        httpCall.request()
    }
}
