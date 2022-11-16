// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class StringAnyAggregationSpec extends Specification {

    String name
    String fieldName
    String type
    Integer maxStringBytes

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "stringAny"
    }

    def "Constructor creation"() {
        when:
        StringAnyAggregation stringAnyAggregation = new StringAnyAggregation(name, fieldName)

        then:
        stringAnyAggregation.name == name
        stringAnyAggregation.fieldName == fieldName
        stringAnyAggregation.getType() == type
        stringAnyAggregation.maxStringBytes == 1024
    }

    def "Constructor creation with max string bytes"() {
        when:
        maxStringBytes = 2048
        StringAnyAggregation stringAnyAggregation = new StringAnyAggregation(name, fieldName, maxStringBytes)

        then:
        stringAnyAggregation.name == name
        stringAnyAggregation.fieldName == fieldName
        stringAnyAggregation.getType() == type
        stringAnyAggregation.maxStringBytes == 2048
    }

    def "String Any aggregation creation with name"() {
        setup:
        StringAnyAggregation stringAnyAggregation = new StringAnyAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        StringAnyAggregation newStringAnyAggregation = stringAnyAggregation.withName(newName)

        then:
        newStringAnyAggregation.name == newName
        newStringAnyAggregation.fieldName == fieldName
        newStringAnyAggregation.getType() == type
    }

    def "String Any aggregation creation with field name"() {
        setup:
        StringAnyAggregation stringAnyAggregation = new StringAnyAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        StringAnyAggregation newStringAnyAggregation = stringAnyAggregation.withFieldName(newFieldName)

        then:
        newStringAnyAggregation.name == name
        newStringAnyAggregation.fieldName == newFieldName
        newStringAnyAggregation.getType() == type
    }

    def "String Any aggregation creation with max string bytes"() {
        setup:
        StringAnyAggregation stringAnyAggregation = new StringAnyAggregation(name, fieldName)
        Integer newMaxStringBytes = 2048

        when:
        StringAnyAggregation newStringAnyAggregation = stringAnyAggregation.withMaxBytes(newMaxStringBytes)

        then:
        newStringAnyAggregation.name == name
        newStringAnyAggregation.fieldName == fieldName
        newStringAnyAggregation.getType() == type
        newStringAnyAggregation.maxStringBytes == 2048
    }
}
