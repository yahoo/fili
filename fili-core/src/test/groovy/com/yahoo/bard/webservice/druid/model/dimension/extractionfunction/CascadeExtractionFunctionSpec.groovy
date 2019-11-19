// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction

import com.yahoo.bard.webservice.application.ObjectMappersSuite

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

class CascadeExtractionFunctionSpec extends Specification {

    ObjectMapper objectMapper

    String expectedSerialization ="""
    {
        "type" : "cascade",
        "extractionFns" : [
            {
                "type" : "test"
            }
        ]
    }
"""
    def setup() {
        objectMapper = new ObjectMappersSuite().getMapper()
    }

    def "Mapping function serializes as expected"() {
        setup:
        ObjectMapper mapper = new ObjectMappersSuite().mapper
        ExtractionFunction extractionFunction = new TestExtractFunction()
        CascadeExtractionFunction cascadeExtractionFunction = new CascadeExtractionFunction([extractionFunction])

        expect:
        mapper.valueToTree(cascadeExtractionFunction) == mapper.readTree(expectedSerialization)
    }
}
