// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction

import com.yahoo.bard.webservice.application.ObjectMappersSuite

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

import java.util.regex.Pattern

class RegularExpressionExtractionFunctionSpec extends Specification {

    ObjectMapper objectMapper

    String expectedSerializationSimple ="""
    {
        "type" : "regex",
        "expr" : "foo"
    }
"""

    String expectedSerializationComplex ="""
    {
        "type" : "regex",
        "expr" : "foo",
        "index": 2,
        "replaceMissingValue": true,
        "replaceMissingValueWith": "bar"
    }
"""

    def setup() {
        objectMapper = new ObjectMappersSuite().getMapper()
    }

    def "Mapping function serializes as expected"() {
        setup:
        ObjectMapper mapper = new ObjectMappersSuite().mapper
        String patternString = "foo"
        Pattern pattern = Pattern.compile(patternString)
        RegularExpressionExtractionFunction simpleFunction = new RegularExpressionExtractionFunction(pattern)
        RegularExpressionExtractionFunction complexFunction = new RegularExpressionExtractionFunction(pattern, 2, "bar")

        expect:
        mapper.valueToTree(simpleFunction) == mapper.readTree(expectedSerializationSimple)
        mapper.valueToTree(complexFunction) == mapper.readTree(expectedSerializationComplex)
    }
}
