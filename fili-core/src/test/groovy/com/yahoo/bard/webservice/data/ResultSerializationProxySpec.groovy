// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Result object serialization test
 */
class ResultSerializationProxySpec extends Specification {

    private static final ObjectMapper objectMapper = new ObjectMappersSuite().getMapper()
    SerializationResources resources
    ResultSerializationProxy serializeResult

    def setup() {
        resources = new SerializationResources().init()
        serializeResult = new ResultSerializationProxy(resources.result2)
    }

    def "Result object custom serialization produces the expected json output"() {
        expect:
        GroovyTestUtils.compareJson(
                resources.serializedResult2,
                objectMapper.writeValueAsString(serializeResult),
                JsonSortStrategy.SORT_BOTH)
    }

    def "Custom serialization of all the dimension rows of a Result produces the expected json output"() {
        setup:
        HashMap dimensionRows = ["ageBracket":"1", "country":"US", "gender":"m"]

        expect:
        serializeResult.getDimensionValues(resources.result1) == dimensionRows
    }

    def "Custom serialization of a null dimension rows of a Result produces the expected json output"() {
        setup:
        HashMap dimensionRows = ["ageBracket":null, "country":null, "gender":null]

        expect:
        serializeResult.getDimensionValues(resources.nullResult) == dimensionRows
    }

    @Unroll
    def "Custom serialization of all the metric rows of a Result produces the expected json output"() {
        setup:
        Map<String, Object> metricValues = serializeResult.getMetricValues(resources.result1)

        expect:
        GroovyTestUtils.compareObjects(metricValues,  ["lookbackPageViews":112, "retentionPageViews":113, "simplePageViews":111])
    }
}
