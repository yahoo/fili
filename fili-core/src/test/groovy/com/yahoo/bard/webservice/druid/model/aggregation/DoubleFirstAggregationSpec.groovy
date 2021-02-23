// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class DoubleFirstAggregationSpec extends Specification {

    String name
    String fieldName
    String type

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "doubleFirst"
    }

    def "Constructor creation"() {
        when:
        DoubleFirstAggregation doubleFirstAggregation = new DoubleFirstAggregation(name, fieldName)

        then:
        doubleFirstAggregation.name == name
        doubleFirstAggregation.fieldName == fieldName
        doubleFirstAggregation.getType() == type
    }

    def "Double first aggregation creation with name"() {
        setup:
        DoubleFirstAggregation doubleFirstAggregation = new DoubleFirstAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        DoubleFirstAggregation newDoubleFirstAggregation = doubleFirstAggregation.withName(newName)

        then:
        newDoubleFirstAggregation.name == newName
        newDoubleFirstAggregation.fieldName == fieldName
        newDoubleFirstAggregation.getType() == type
    }

    def "Double first aggregation creation with field name"() {
        setup:
        DoubleFirstAggregation doubleFirstAggregation = new DoubleFirstAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        DoubleFirstAggregation newDoubleFirstAggregation = doubleFirstAggregation.withFieldName(newFieldName)

        then:
        newDoubleFirstAggregation.name == name
        newDoubleFirstAggregation.fieldName == newFieldName
        newDoubleFirstAggregation.getType() == type
    }
}
