// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class StringLastAggregationSpec extends Specification {

    String name
    String fieldName
    String type
    Integer maxStringBytes

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "stringLast"
    }

    def "Constructor creation"() {
        when:
        StringLastAggregation stringLastAggregation = new StringLastAggregation(name, fieldName)

        then:
        stringLastAggregation.name == name
        stringLastAggregation.fieldName == fieldName
        stringLastAggregation.getType() == type
        stringLastAggregation.maxStringBytes == 1024
    }

    def "Constructor creation with max string bytes"() {
        when:
        maxStringBytes = 2048
        StringLastAggregation stringLastAggregation = new StringLastAggregation(name, fieldName, maxStringBytes)

        then:
        stringLastAggregation.name == name
        stringLastAggregation.fieldName == fieldName
        stringLastAggregation.getType() == type
        stringLastAggregation.maxStringBytes == 2048
    }

    def "String Last aggregation creation with name"() {
        setup:
        StringLastAggregation stringLastAggregation = new StringLastAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        StringLastAggregation newStringLastAggregation = stringLastAggregation.withName(newName)

        then:
        newStringLastAggregation.name == newName
        newStringLastAggregation.fieldName == fieldName
        newStringLastAggregation.getType() == type
    }

    def "String Last aggregation creation with field name"() {
        setup:
        StringLastAggregation stringLastAggregation = new StringLastAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        StringLastAggregation newStringLastAggregation = stringLastAggregation.withFieldName(newFieldName)

        then:
        newStringLastAggregation.name == name
        newStringLastAggregation.fieldName == newFieldName
        newStringLastAggregation.getType() == type
    }

    def "String Last aggregation creation with max string bytes"() {
        setup:
        StringLastAggregation stringLastAggregation = new StringLastAggregation(name, fieldName)
        Integer newMaxStringBytes = 2048

        when:
        StringLastAggregation newStringLastAggregation = stringLastAggregation.withMaxBytes(newMaxStringBytes)

        then:
        newStringLastAggregation.name == name
        newStringLastAggregation.fieldName == fieldName
        newStringLastAggregation.getType() == type
        newStringLastAggregation.maxStringBytes == 2048
    }
}
