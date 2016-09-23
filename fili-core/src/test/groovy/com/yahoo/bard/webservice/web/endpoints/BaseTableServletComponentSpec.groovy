// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Specification
import spock.lang.Timeout

@Timeout(30)
// Fail test if hangs
abstract class BaseTableServletComponentSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    JerseyTestBinder jtb

    abstract Class<?>[] getResourceClasses()

    abstract String getTarget()

    Map<String, List<String>> getQueryParams() { [:] }

    abstract String getExpectedApiResponse()

    def setup() {
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(getResourceClasses())
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    /**
     * Make sure that the String is valid JSON by parsing it into a JSON object.
     *
     * @param json String to try to parse as JSON.
     */
    static void validateJson(String json) {
        MAPPER.readTree(json)
    }

    def "test api response"() {
        given: "A known response"
        validateJson(getExpectedApiResponse())

        when: "We send a request"
        String result = makeAbstractRequest()

        then: "The response is what we expect"
        GroovyTestUtils.compareJson(result, getExpectedApiResponse(), JsonSortStrategy.SORT_BOTH)
    }

    String makeAbstractRequest() {
        // Set target of call
        def httpCall = jtb.getHarness().target(getTarget())

        // Add query params to call
        getQueryParams()?.each { String key, List<String> values ->
            httpCall = httpCall.queryParam(key, values.join(","))
        }

        // Make the call
        httpCall.request().get(String.class)
    }
}
