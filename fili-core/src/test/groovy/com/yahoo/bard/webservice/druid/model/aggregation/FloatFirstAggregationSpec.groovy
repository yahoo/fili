// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class FloatFirstAggregationSpec extends Specification {

    String name
    String fieldName
    String type

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "floatFirst"
    }

    def "Constructor creation"() {
        when:
        FloatFirstAggregation floatFirstAggregation = new FloatFirstAggregation(name, fieldName)

        then:
        floatFirstAggregation.name == name
        floatFirstAggregation.fieldName == fieldName
        floatFirstAggregation.getType() == type
    }

    def "Float first aggregation creation with name"() {
        setup:
        FloatFirstAggregation floatFirstAggregation = new FloatFirstAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        FloatFirstAggregation newFloatFirstAggregation = floatFirstAggregation.withName(newName)

        then:
        newFloatFirstAggregation.name == newName
        newFloatFirstAggregation.fieldName == fieldName
        newFloatFirstAggregation.getType() == type
    }

    def "Float first aggregation creation with field name"() {
        setup:
        FloatFirstAggregation floatFirstAggregation = new FloatFirstAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        FloatFirstAggregation newFloatFirstAggregation = floatFirstAggregation.withFieldName(newFieldName)

        then:
        newFloatFirstAggregation.name == name
        newFloatFirstAggregation.fieldName == newFieldName
        newFloatFirstAggregation.getType() == type
    }
}
