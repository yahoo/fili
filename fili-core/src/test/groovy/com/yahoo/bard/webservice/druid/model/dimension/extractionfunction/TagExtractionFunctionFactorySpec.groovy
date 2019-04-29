// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction

import com.yahoo.bard.webservice.application.ObjectMappersSuite

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Unroll

import java.util.regex.Pattern

class TagExtractionFunctionFactorySpec extends Specification {

    String expectedSerialization = """
        {
            "type" : "cascade",
            "extractionFns" : [
                {
                    "type":"regex",
                    "expr":"(.+,)*(11)(,.+)*",
                    "index":2,
                    "replaceMissingValue":true,
                    "replaceMissingValueWith":""
                },
                {
                    "type" : "lookup",
                    "lookup" : 
                    {
                        "type" : "map",
                        "map" : {"11":"Yes"}
                    },
                    "retainMissingValue" : false,
                    "replaceMissingValueWith" : "No",
                    "injective" : false,
                    "optimize" : false
                }
            ]
        }
"""


    def "Mapping function serializes as expected"() {
        setup:
        ObjectMapper mapper = new ObjectMappersSuite().mapper
        ExtractionFunction function = TagExtractionFunctionFactory.buildTagExtractionFunction("11");

        expect:
        mapper.valueToTree(function) == mapper.readTree(expectedSerialization)
    }

    def "Empty string tag value marked as invalid"() {
        setup:
        ObjectMapper mapper = new ObjectMappersSuite().mapper

        when:
        ExtractionFunction function = TagExtractionFunctionFactory.buildTagExtractionFunction("");

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def "expression #expression matches #value is #expected"() {
        expect:
        String patternString = String.format(TagExtractionFunctionFactory.DEFAULT_TAG_REG_EX_FORMAT, expression);
        Pattern.compile(patternString).matcher(value).matches() == expected

        where:
        value      | expression | expected
        "11,12,13" | "11"       | true
        "11"       | "11"       | true
        "10,11"    | "11"       | true
        "111"      | "11"       | false
    }
}
