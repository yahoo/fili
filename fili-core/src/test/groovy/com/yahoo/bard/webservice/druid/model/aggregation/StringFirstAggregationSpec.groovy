// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class StringFirstAggregationSpec extends Specification {

    String name
    String fieldName
    String type
    Integer maxStringBytes

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "stringFirst"
    }

    def "Constructor creation"() {
        when:
        StringFirstAggregation stringFirstAggregation = new StringFirstAggregation(name, fieldName)

        then:
        stringFirstAggregation.name == name
        stringFirstAggregation.fieldName == fieldName
        stringFirstAggregation.getType() == type
        stringFirstAggregation.maxStringBytes == 1024
    }

    def "Constructor creation with max string bytes"() {
        when:
        maxStringBytes = 2048
        StringFirstAggregation stringFirstAggregation = new StringFirstAggregation(name, fieldName, maxStringBytes)

        then:
        stringFirstAggregation.name == name
        stringFirstAggregation.fieldName == fieldName
        stringFirstAggregation.getType() == type
        stringFirstAggregation.maxStringBytes == 2048
    }

    def "String first aggregation creation with name"() {
        setup:
        StringFirstAggregation stringFirstAggregation = new StringFirstAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        StringFirstAggregation newStringFirstAggregation = stringFirstAggregation.withName(newName)

        then:
        newStringFirstAggregation.name == newName
        newStringFirstAggregation.fieldName == fieldName
        newStringFirstAggregation.getType() == type
    }

    def "String first aggregation creation with field name"() {
        setup:
        StringFirstAggregation stringFirstAggregation = new StringFirstAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        StringFirstAggregation newStringFirstAggregation = stringFirstAggregation.withFieldName(newFieldName)

        then:
        newStringFirstAggregation.name == name
        newStringFirstAggregation.fieldName == newFieldName
        newStringFirstAggregation.getType() == type
    }

    def "String first aggregation creation with max string bytes"() {
        setup:
        StringFirstAggregation stringFirstAggregation = new StringFirstAggregation(name, fieldName)
        Integer newMaxStringBytes = 2048

        when:
        StringFirstAggregation newStringFirstAggregation = stringFirstAggregation.withMaxBytes(newMaxStringBytes)

        then:
        newStringFirstAggregation.name == name
        newStringFirstAggregation.fieldName == fieldName
        newStringFirstAggregation.getType() == type
        newStringFirstAggregation.maxStringBytes == 2048
    }
}
