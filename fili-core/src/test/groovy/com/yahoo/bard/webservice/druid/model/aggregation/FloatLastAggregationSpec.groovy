// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import spock.lang.Specification

class FloatLastAggregationSpec extends Specification {

    String name
    String fieldName
    String type

    def setup() {
        name = "foo"
        fieldName = "bar"
        type = "floatLast"
    }

    def "Constructor creation"() {
        when:
        FloatLastAggregation floatLastAggregation = new FloatLastAggregation(name, fieldName)

        then:
        floatLastAggregation.name == name
        floatLastAggregation.fieldName == fieldName
        floatLastAggregation.getType() == type
    }

    def "Float Last aggregation creation with name"() {
        setup:
        FloatLastAggregation floatLastAggregation = new FloatLastAggregation(name, fieldName)
        String newName = "fooPage"

        when:
        FloatLastAggregation newFloatLastAggregation = floatLastAggregation.withName(newName)

        then:
        newFloatLastAggregation.name == newName
        newFloatLastAggregation.fieldName == fieldName
        newFloatLastAggregation.getType() == type
    }

    def "Float Last aggregation creation with field name"() {
        setup:
        FloatLastAggregation floatLastAggregation = new FloatLastAggregation(name, fieldName)
        String newFieldName = "barPage"

        when:
        FloatLastAggregation newFloatLastAggregation = floatLastAggregation.withFieldName(newFieldName)

        then:
        newFloatLastAggregation.name == name
        newFloatLastAggregation.fieldName == newFieldName
        newFloatLastAggregation.getType() == type
    }
}
