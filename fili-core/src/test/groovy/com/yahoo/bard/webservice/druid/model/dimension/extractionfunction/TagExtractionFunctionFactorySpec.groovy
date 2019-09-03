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
                    "expr":"^(.+,)*(11)(,.+)*\$",
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
        List<ExtractionFunction> function = TagExtractionFunctionFactory.buildTagExtractionFunction("11")

        expect: "wrap in a cascade extraction function because that is how a chain of extraction functions is serialized"
        mapper.valueToTree(new CascadeExtractionFunction(function)) == mapper.readTree(expectedSerialization)
    }

    def "Empty string tag value marked as invalid"() {
        when:
        TagExtractionFunctionFactory.buildTagExtractionFunction("")

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def "expression #expression matches #value is #expected"() {
        expect:
        String patternString = String.format(TagExtractionFunctionFactory.DEFAULT_TAG_REG_EX_FORMAT, expression)
        Pattern.compile(patternString).matcher(value).matches() == expected

        where:
        expression | value      || expected
        "11"       | "11,12,13" || true
        "11"       | "11"       || true
        "11"       | "10,11"    || true
        "11"       | "111"      || false
        "1"        | "2,3,12"   || false
        "1"        | "21,2,3"   || false

    }
}
