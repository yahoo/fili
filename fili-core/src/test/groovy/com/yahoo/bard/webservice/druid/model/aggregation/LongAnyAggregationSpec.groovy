// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class LongAnyAggregationSpec extends Specification {

    String name
    String fieldName
    String type

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "longAny"
    }

    def "Constructor creation"() {
        when:
        LongAnyAggregation longAnyAggregation = new LongAnyAggregation(name, fieldName)

        then:
        longAnyAggregation.name == name
        longAnyAggregation.fieldName == fieldName
        longAnyAggregation.getType() == type
    }

    def "Long Any aggregation creation with name"() {
        setup:
        LongAnyAggregation longAnyAggregation = new LongAnyAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        LongAnyAggregation newLongAnyAggregation = longAnyAggregation.withName(newName)

        then:
        newLongAnyAggregation.name == newName
        newLongAnyAggregation.fieldName == fieldName
        newLongAnyAggregation.getType() == type
    }

    def "Long Any aggregation creation with field name"() {
        setup:
        LongAnyAggregation longAnyAggregation = new LongAnyAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        LongAnyAggregation newLongAnyAggregation = longAnyAggregation.withFieldName(newFieldName)

        then:
        newLongAnyAggregation.name == name
        newLongAnyAggregation.fieldName == newFieldName
        newLongAnyAggregation.getType() == type
    }
}
