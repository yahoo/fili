// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class DoubleLastAggregationSpec extends Specification {

    String name
    String fieldName
    String type

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "doubleLast"
    }

    def "Constructor creation"() {
        when:
        DoubleLastAggregation doubleLastAggregation = new DoubleLastAggregation(name, fieldName)

        then:
        doubleLastAggregation.name == name
        doubleLastAggregation.fieldName == fieldName
        doubleLastAggregation.getType() == type
    }

    def "Double first aggregation creation with name"() {
        setup:
        DoubleLastAggregation doubleLastAggregation = new DoubleLastAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        DoubleLastAggregation newDoubleLasttAggregation = doubleLastAggregation.withName(newName)

        then:
        newDoubleLasttAggregation.name == newName
        newDoubleLasttAggregation.fieldName == fieldName
        newDoubleLasttAggregation.getType() == type
    }

    def "Double first aggregation creation with field name"() {
        setup:
        DoubleLastAggregation doubleLastAggregation = new DoubleLastAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        DoubleLastAggregation newDoubleLasttAggregation = doubleLastAggregation.withFieldName(newFieldName)

        then:
        newDoubleLasttAggregation.name == name
        newDoubleLasttAggregation.fieldName == newFieldName
        newDoubleLasttAggregation.getType() == type
    }
}
