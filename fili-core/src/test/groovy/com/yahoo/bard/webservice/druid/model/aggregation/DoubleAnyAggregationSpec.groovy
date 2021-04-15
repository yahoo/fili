// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class DoubleAnyAggregationSpec extends Specification {

    String name
    String fieldName
    String type

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "doubleAny"
    }

    def "Constructor creation"() {
        when:
        DoubleAnyAggregation doubleAnyAggregation = new DoubleAnyAggregation(name, fieldName)

        then:
        doubleAnyAggregation.name == name
        doubleAnyAggregation.fieldName == fieldName
        doubleAnyAggregation.getType() == type
    }

    def "Double Any aggregation creation with name"() {
        setup:
        DoubleAnyAggregation doubleAnyAggregation = new DoubleAnyAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        DoubleAnyAggregation newDoubleAnyAggregation = doubleAnyAggregation.withName(newName)

        then:
        newDoubleAnyAggregation.name == newName
        newDoubleAnyAggregation.fieldName == fieldName
        newDoubleAnyAggregation.getType() == type
    }

    def "Double Any aggregation creation with field name"() {
        setup:
        DoubleAnyAggregation doubleAnyAggregation = new DoubleAnyAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        DoubleAnyAggregation newDoubleAnyAggregation = doubleAnyAggregation.withFieldName(newFieldName)

        then:
        newDoubleAnyAggregation.name == name
        newDoubleAnyAggregation.fieldName == newFieldName
        newDoubleAnyAggregation.getType() == type
    }
}
