// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Specification

class DebugDataServletSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    JerseyTestBinder jtb

    def setup() {
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(getResourceClasses())
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    void validateJson(String json) {
        MAPPER.readTree(json)
    }

    def "test api response"() {
        given: "An expected API response"
        validateJson(getExpectedApiResponse())

        when: "We send a request"
        String result = makeAbstractRequest()

        then: "The response rows are what we expect"
        GroovyTestUtils.compareErrorPayload(result, getExpectedApiResponse())
    }

    String makeAbstractRequest() {
        // Set target of call
        def httpCall = jtb.getHarness().target(getTarget())

        // Add query params to call
        getQueryParams().each { String key, List<String> values ->
            httpCall = httpCall.queryParam(key, values.join(","))
        }

        // Make the call
        httpCall.request().get(String.class)
    }

    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    String getTarget() {
        return "data/shapes/day/color/"
    }

    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["depth"],
                "dateTime": ["2014-06-02%2F2014-06-03"],
                "format"  : ["debug"]
        ]
    }

    String getExpectedApiResponse() {
        """{
              "description": "DEBUG",
              "druidQuery": {
                "aggregations": [
                  {
                    "fieldName": "depth",
                    "name": "depth",
                    "type": "longSum"
                  }
                ],
                "dataSource": {
                  "name": "color_shapes",
                  "type": "table"
                },
                "dimensions": ["color"],
                "granularity": ${BaseDataServletComponentSpec.getTimeGrainString()},
                "intervals": [
                  "2014-06-02T00:00:00.000Z/2014-06-03T00:00:00.000Z"
                ],
                "postAggregations": [],
                "queryType": "groupBy",
                "context": {}
              },
              "reason": "DEBUG",
              "status": 200,
              "statusName": "OK"
            }"""
    }
}
